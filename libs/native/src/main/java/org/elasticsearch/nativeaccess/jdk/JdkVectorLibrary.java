/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.DataType;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Function;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Operation;
import org.elasticsearch.nativeaccess.lib.LoaderHelper;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.nio.channels.FileChannel;
import java.util.Objects;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.functionAddressOrNull;

public final class JdkVectorLibrary implements VectorLibrary {

    static final Logger logger = LogManager.getLogger(JdkVectorLibrary.class);

    private record OperationSignature(Function function, DataType dataType, Operation operation) {}

    private static final Map<OperationSignature, MethodHandle> HANDLES;

    public static final JdkVectorSimilarityFunctions INSTANCE;

    /**
     * Native functions in the native simdvec library can have multiple implementations, one for each "capability level".
     * A capability level of "0" means that there is no native function for that platform.
     * Functions for the base ("1") level are exposed with a simple function name (e.g. "vec_dot7u")
     * Functions for the more advanced levels (2, 3, ...) are exported with a name "decorated" by adding the capability level as
     * a suffix: if the capability level is N, the suffix will be "_N" (e.g. "vec_dot7u_2").
     * Capability levels maps to the availability of advanced vector instructions sets for a platform. For example, for x64 we currently
     * define 2 capability levels, 1 (base, processor supports AVX2) and 2 (processor supports AVX-512 with VNNI and VPOPCNT).
     * <p>
     * This function binds the function with the highest capability level exported by the native library by performing fallback lookups:
     * starting from the supported capability level N, it looks up function_N, function_{N-1}... function.
     *
     * @param functionName          the base function name, as exported by the native library
     * @param capability            the capability level supported by this platform, as returned by `int vec_caps()`
     * @param functionDescriptor    the function descriptor for the function(s) starting with `functionName`
     * @return a {@link MethodHandle} to the native function
     */
    private static MethodHandle bindFunction(String functionName, int capability, FunctionDescriptor functionDescriptor) {
        for (int caps = capability; caps > 0; --caps) {
            var suffix = caps > 1 ? "_" + caps : "";
            var fullFunctionName = functionName + suffix;
            logger.trace("Lookup for {}", fullFunctionName);
            var function = functionAddressOrNull(functionName + suffix);
            if (function != null) {
                logger.debug("Binding {}", fullFunctionName);
                return downcallHandle(function, functionDescriptor, LinkerHelperUtil.critical());
            }
        }
        throw new LinkageError("Native function [" + functionName + "] could not be found");
    }

    static {
        LoaderHelper.loadLibrary("vec");
        MethodHandle vecCaps$mh = downcallHandle("vec_caps", FunctionDescriptor.of(JAVA_INT));
        Map<OperationSignature, MethodHandle> handles = new HashMap<>();

        try {
            int caps = (int) vecCaps$mh.invokeExact();
            logger.info("vec_caps=" + caps);
            if (caps > 0) {
                FunctionDescriptor intSingle = FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT);
                FunctionDescriptor floatSingle = FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT);
                FunctionDescriptor bulk = FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS);
                FunctionDescriptor bulkOffsets = FunctionDescriptor.ofVoid(
                    ADDRESS,
                    ADDRESS,
                    JAVA_INT,
                    JAVA_INT,
                    ADDRESS,
                    JAVA_INT,
                    ADDRESS
                );

                for (Function f : Function.values()) {
                    String funcName = switch (f) {
                        case DOT_PRODUCT -> "dot";
                        case SQUARE_DISTANCE -> "sqr";
                    };

                    for (DataType type : DataType.values()) {
                        String typeName = switch (type) {
                            case INT7 -> "7u";
                            case FLOAT32 -> "f32";
                        };

                        for (Operation op : Operation.values()) {
                            String opName = switch (op) {
                                case SINGLE -> "";
                                case BULK -> "_bulk";
                                case BULK_OFFSETS -> "_bulk_offsets";
                            };

                            FunctionDescriptor descriptor = switch (op) {
                                case SINGLE -> switch (type) {
                                    case INT7 -> intSingle;
                                    case FLOAT32 -> floatSingle;
                                };
                                case BULK -> bulk;
                                case BULK_OFFSETS -> bulkOffsets;
                            };

                            MethodHandle handle = bindFunction("vec_" + funcName + typeName + opName, caps, descriptor);
                            handles.put(new OperationSignature(f, type, op), handle);
                        }
                    }
                }

                HANDLES = Collections.unmodifiableMap(handles);
                INSTANCE = new JdkVectorSimilarityFunctions();
            } else {
                if (caps < 0) {
                    logger.warn("""
                        Your CPU supports vector capabilities, but they are disabled at OS level. For optimal performance, \
                        enable them in your OS/Hypervisor/VM/container""");
                }
                HANDLES = null;
                INSTANCE = null;
            }
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    public JdkVectorLibrary() {}

    @Override
    public VectorSimilarityFunctions getVectorSimilarityFunctions() {
        return INSTANCE;
    }

    private static final class JdkVectorSimilarityFunctions implements VectorSimilarityFunctions {

        /**
         * Invokes a similarity function between 1 "query" vector and a single "target" vector (as opposed to N target vectors in a bulk
         * operation). The native function parameters are handled so to avoid the cost of shared MemorySegment checks, by reinterpreting
         * the MemorySegment with a new local scope.
         * <p>
         * Vector data is consumed by native functions directly via a pointer to contiguous memory, represented in FFI by
         * {@link MemorySegment}s, which safely encapsulate a memory location, off-heap or on-heap.
         * We mainly use <b>shared</b> MemorySegments for off-heap vectors (via {@link Arena#ofShared} or via {@link FileChannel#map}).
         * <p>
         * Shared MemorySegments have a built-in check for liveness when accessed by native functions, implemented by JIT adding some
         * additional instructions before/after the native function is actually called.
         * While the cost of these instructions is usually negligible, single score distance functions are so heavily optimized that can
         * execute in less than 50 CPU cycles, so every overhead shows. In contrast, there is no need to worry in the case of
         * bulk functions, as the call cost is amortized over hundred or thousands of vectors and is practically invisible.
         * <p>
         * By reinterpreting the input MemorySegments with a new local scope, the JVM does not inject any additional check.
         * Benchmarks show that this gives us a boost of ~15% on x64 and ~5% on ARM for single vector distance functions.
         * @param mh        the {@link MethodHandle} of the "single" distance function to invoke
         * @param a         the {@link MemorySegment} for the first vector (first parameter to pass to the native function)
         * @param b         the {@link MemorySegment} for the second vector (second parameter to pass to the native function)
         * @param length    the vectors length (third parameter to pass to the native function)
         * @return          the distance as computed by the native function
         */
        private static long callSingleDistanceLong(MethodHandle mh, MemorySegment a, MemorySegment b, int length) {
            try (var arena = Arena.ofConfined()) {
                var aSegment = a.isNative() ? a.reinterpret(arena, null) : a;
                var bSegment = b.isNative() ? b.reinterpret(arena, null) : b;
                return (long) mh.invokeExact(aSegment, bSegment, length);
            } catch (Throwable t) {
                throw invocationError(t, a, b);
            } finally {
                assert a.scope().isAlive();
                assert b.scope().isAlive();
            }
        }

        /** See {@link JdkVectorSimilarityFunctions#callSingleDistanceLong} */
        private static int callSingleDistanceInt(MethodHandle mh, MemorySegment a, MemorySegment b, int length) {
            try (var arena = Arena.ofConfined()) {
                var aSegment = a.isNative() ? a.reinterpret(arena, null) : a;
                var bSegment = b.isNative() ? b.reinterpret(arena, null) : b;
                return (int) mh.invokeExact(aSegment, bSegment, length);
            } catch (Throwable t) {
                throw invocationError(t, a, b);
            } finally {
                assert a.scope().isAlive();
                assert b.scope().isAlive();
            }
        }

        /** See {@link JdkVectorSimilarityFunctions#callSingleDistanceLong} */
        private static float callSingleDistanceFloat(MethodHandle mh, MemorySegment a, MemorySegment b, int length) {
            try (var arena = Arena.ofConfined()) {
                var aSegment = a.isNative() ? a.reinterpret(arena, null) : a;
                var bSegment = b.isNative() ? b.reinterpret(arena, null) : b;
                return (float) mh.invokeExact(aSegment, bSegment, length);
            } catch (Throwable t) {
                throw invocationError(t, a, b);
            } finally {
                assert a.scope().isAlive();
                assert b.scope().isAlive();
            }
        }

        private static Error invocationError(Throwable t, MemorySegment segment1, MemorySegment segment2) {
            String msg = Strings.format(
                "Invocation failed: first segment=[%s, scope=%s, isAlive=%b], second segment=[%s, scope=%s, isAlive=%b]",
                segment1,
                segment1.scope(),
                segment1.scope().isAlive(),
                segment2,
                segment2.scope(),
                segment2.scope().isAlive()
            );
            return new AssertionError(msg, t);
        }

        static boolean checkSingleInt(MemorySegment a, MemorySegment b, int length) {
            if (a.byteSize() != b.byteSize()) {
                throw new IllegalArgumentException("Dimensions differ: " + a.byteSize() + " != " + b.byteSize());
            }
            Objects.checkFromIndexSize(0, length, (int) a.byteSize());
            return true;
        }

        static boolean checkSingleFloat(MemorySegment a, MemorySegment b, int length) {
            if (a.byteSize() != b.byteSize()) {
                throw new IllegalArgumentException(
                    "Dimensions differ: " + a.byteSize() / Float.BYTES + " != " + b.byteSize() / Float.BYTES
                );
            }
            Objects.checkFromIndexSize(0, length * Float.BYTES, (int) a.byteSize());
            return true;
        }

        static boolean checkBulk(int elementSize, MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            Objects.checkFromIndexSize(0, length * count * elementSize, (int) a.byteSize());
            Objects.checkFromIndexSize(0, length, (int) b.byteSize());
            Objects.checkFromIndexSize(0, count * Float.BYTES, (int) result.byteSize());
            return true;
        }

        static boolean checkBulkOffsets(
            int elementSize,
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            if ((pitch % elementSize) != 0) throw new IllegalArgumentException("Pitch needs to be a multiple of " + elementSize);
            return true;
        }

        private static final Map<OperationSignature, MethodHandle> HANDLES_WITH_CHECKS;

        static {
            MethodHandles.Lookup lookup = MethodHandles.lookup();

            try {
                Map<OperationSignature, MethodHandle> handlesWithChecks = new HashMap<>(HANDLES);

                for (var op : handlesWithChecks.entrySet()) {
                    MethodHandle checkedHandle = switch (op.getKey().operation()) {
                        case SINGLE -> {
                            MethodType checkMethodType = MethodType.methodType(
                                boolean.class,
                                MemorySegment.class,
                                MemorySegment.class,
                                int.class
                            );
                            MethodHandle checkMethod = switch (op.getKey().dataType()) {
                                case INT7 -> lookup.findStatic(JdkVectorSimilarityFunctions.class, "checkSingleInt", checkMethodType);
                                case FLOAT32 -> lookup.findStatic(JdkVectorSimilarityFunctions.class, "checkSingleFloat", checkMethodType);
                            };
                            MethodHandle callMethod = switch (op.getKey().dataType()) {
                                case INT7 -> lookup.findStatic(JdkVectorSimilarityFunctions.class, "callSingleDistanceInt", MethodType.methodType(
                                    int.class, MethodHandle.class, MemorySegment.class, MemorySegment.class, int.class
                                ));
                                case FLOAT32 -> lookup.findStatic(JdkVectorSimilarityFunctions.class, "callSingleDistanceFloat", MethodType.methodType(
                                    float.class, MethodHandle.class, MemorySegment.class, MemorySegment.class, int.class
                                ));
                            };
                            yield MethodHandles.guardWithTest(
                                checkMethod,
                                MethodHandles.insertArguments(callMethod, 0, op.getValue()), MethodHandles.empty(op.getValue().type()));
                        }
                        case BULK -> {
                            MethodHandle checkMethod = lookup.findStatic(
                                JdkVectorSimilarityFunctions.class,
                                "checkBulk",
                                MethodType.methodType(
                                    boolean.class,
                                    int.class,
                                    MemorySegment.class,
                                    MemorySegment.class,
                                    int.class,
                                    int.class,
                                    MemorySegment.class
                                )
                            );
                            yield MethodHandles.guardWithTest(
                                MethodHandles.insertArguments(checkMethod, 0, op.getKey().dataType().bytes()),
                                op.getValue(),
                                MethodHandles.empty(op.getValue().type())
                            );
                        }
                        case BULK_OFFSETS -> {
                            MethodHandle checkMethod = lookup.findStatic(
                                JdkVectorSimilarityFunctions.class,
                                "checkBulkOffsets",
                                MethodType.methodType(
                                    boolean.class,
                                    int.class,
                                    MemorySegment.class,
                                    MemorySegment.class,
                                    int.class,
                                    int.class,
                                    MemorySegment.class,
                                    int.class,
                                    MemorySegment.class
                                )
                            );
                            yield MethodHandles.guardWithTest(
                                MethodHandles.insertArguments(checkMethod, 0, op.getKey().dataType().bytes()),
                                op.getValue(),
                                MethodHandles.empty(op.getValue().type())
                            );
                        }
                    };

                    op.setValue(checkedHandle);
                }

                HANDLES_WITH_CHECKS = Collections.unmodifiableMap(handlesWithChecks);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public MethodHandle getHandle(Function function, DataType dataType, Operation operation) {
            return HANDLES_WITH_CHECKS.get(new OperationSignature(function, dataType, operation));
        }
    }
}
