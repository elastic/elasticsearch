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
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.BBQType;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.DataType;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Function;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Operation;
import org.elasticsearch.nativeaccess.lib.LoaderHelper;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.functionAddressOrNull;

public final class JdkVectorLibrary implements VectorLibrary {

    static final Logger logger = LogManager.getLogger(JdkVectorLibrary.class);

    private record OperationSignature<E extends Enum<E>>(Function function, E dataType, Operation operation) {}

    private static final Map<OperationSignature<?>, MethodHandle> HANDLES;

    static final MethodHandle applyCorrectionsEuclideanBulk$mh;
    static final MethodHandle applyCorrectionsMaxInnerProductBulk$mh;
    static final MethodHandle applyCorrectionsDotProductBulk$mh;

    private static final JdkVectorSimilarityFunctions INSTANCE;

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
        Map<OperationSignature<?>, MethodHandle> handles = new HashMap<>();

        try {
            int caps = (int) vecCaps$mh.invokeExact();
            logger.info("vec_caps=" + caps);
            if (caps > 0) {
                FunctionDescriptor intSingle = FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT);
                FunctionDescriptor longSingle = FunctionDescriptor.of(JAVA_LONG, ADDRESS, ADDRESS, JAVA_INT);
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
                        case COSINE -> "cos";
                        case DOT_PRODUCT -> "dot";
                        case SQUARE_DISTANCE -> "sqr";
                    };

                    for (Operation op : Operation.values()) {
                        String opName = switch (op) {
                            case SINGLE -> "";
                            case BULK -> "_bulk";
                            case BULK_OFFSETS -> "_bulk_offsets";
                        };

                        for (DataType type : DataType.values()) {
                            // Only byte vectors have cosine
                            // as floats are normalized to unit length to use dot_product instead
                            if (f == Function.COSINE && type != DataType.INT8) continue;

                            String typeName = switch (type) {
                                case INT7U -> "i7u";
                                case INT8 -> "i8";
                                case FLOAT32 -> "f32";
                            };

                            FunctionDescriptor descriptor = switch (op) {
                                case SINGLE -> switch (type) {
                                    case INT7U -> intSingle;
                                    case INT8, FLOAT32 -> floatSingle;
                                };
                                case BULK -> bulk;
                                case BULK_OFFSETS -> bulkOffsets;
                            };

                            MethodHandle handle = bindFunction("vec_" + funcName + typeName + opName, caps, descriptor);
                            handles.put(new OperationSignature<>(f, type, op), handle);
                        }

                        for (BBQType type : BBQType.values()) {
                            // not implemented yet...
                            if (f == Function.COSINE || f == Function.SQUARE_DISTANCE) continue;

                            String typeName = switch (type) {
                                case D1Q4 -> "d1q4";
                                case D2Q4 -> "d2q4";
                                case D4Q4 -> "d4q4";
                            };

                            FunctionDescriptor descriptor = switch (op) {
                                case SINGLE -> longSingle;
                                case BULK -> bulk;
                                case BULK_OFFSETS -> bulkOffsets;
                            };

                            MethodHandle handle = bindFunction("vec_" + funcName + typeName + opName, caps, descriptor);
                            handles.put(new OperationSignature<>(f, type, op), handle);
                        }
                    }
                }

                HANDLES = Collections.unmodifiableMap(handles);

                FunctionDescriptor score = FunctionDescriptor.of(
                    JAVA_FLOAT,
                    ADDRESS, // corrections
                    JAVA_INT, // bulkSize,
                    JAVA_INT, // dimensions,
                    JAVA_FLOAT, // queryLowerInterval,
                    JAVA_FLOAT, // queryUpperInterval,
                    JAVA_INT, // queryComponentSum,
                    JAVA_FLOAT, // queryAdditionalCorrection,
                    JAVA_FLOAT, // queryBitScale,
                    JAVA_FLOAT, // indexBitScale,
                    JAVA_FLOAT, // centroidDp,
                    ADDRESS // scores
                );

                applyCorrectionsEuclideanBulk$mh = bindFunction("diskbbq_apply_corrections_euclidean_bulk", caps, score);
                applyCorrectionsMaxInnerProductBulk$mh = bindFunction("diskbbq_apply_corrections_maximum_inner_product_bulk", caps, score);
                applyCorrectionsDotProductBulk$mh = bindFunction("diskbbq_apply_corrections_dot_product_bulk", caps, score);

                INSTANCE = new JdkVectorSimilarityFunctions();
            } else {
                if (caps < 0) {
                    logger.warn("""
                        Your CPU supports vector capabilities, but they are disabled at OS level. For optimal performance, \
                        enable them in your OS/Hypervisor/VM/container""");
                }
                HANDLES = null;
                applyCorrectionsEuclideanBulk$mh = null;
                applyCorrectionsMaxInnerProductBulk$mh = null;
                applyCorrectionsDotProductBulk$mh = null;
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
         * operation).
         *
         * @param mh        the {@link MethodHandle} of the "single" distance function to invoke
         * @param a         the {@link MemorySegment} for the first vector (first parameter to pass to the native function)
         * @param b         the {@link MemorySegment} for the second vector (second parameter to pass to the native function)
         * @param length    the vectors length (third parameter to pass to the native function)
         * @return          the distance as computed by the native function
         */
        private static long callSingleDistanceLong(MethodHandle mh, MemorySegment a, MemorySegment b, int length) {
            try {
                return (long) mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw invocationError(t, a, b);
            }
        }

        /** See {@link JdkVectorSimilarityFunctions#callSingleDistanceLong} */
        private static int callSingleDistanceInt(MethodHandle mh, MemorySegment a, MemorySegment b, int length) {
            try {
                return (int) mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw invocationError(t, a, b);
            }
        }

        /** See {@link JdkVectorSimilarityFunctions#callSingleDistanceLong} */
        private static float callSingleDistanceFloat(MethodHandle mh, MemorySegment a, MemorySegment b, int length) {
            try {
                return (float) mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw invocationError(t, a, b);
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

        static boolean checkBulk(int elementSize, MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            Objects.checkFromIndexSize(0, length * count * elementSize, (int) a.byteSize());
            Objects.checkFromIndexSize(0, length, (int) b.byteSize());
            Objects.checkFromIndexSize(0, count * Float.BYTES, (int) result.byteSize());
            return true;
        }

        static boolean checkBBQBulk(
            int dataBits,
            MemorySegment dataset,
            MemorySegment query,
            int datasetVectorLengthInBytes,
            int count,
            MemorySegment result
        ) {
            final int queryBits = 4;
            Objects.checkFromIndexSize(0, datasetVectorLengthInBytes * count, (int) dataset.byteSize());
            // 1 bit data -> x4 bits query, 2 bit data -> x2 bits query
            Objects.checkFromIndexSize(0, datasetVectorLengthInBytes * (queryBits / dataBits), (int) query.byteSize());
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
            // TODO: more checks copied from checkBulk
            if ((pitch % elementSize) != 0) throw new IllegalArgumentException("Pitch needs to be a multiple of " + elementSize);
            return true;
        }

        static boolean checkBBQBulkOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            return true;
        }

        private static final MethodHandle dotI7uHandle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT7U, Operation.SINGLE)
        );

        static int dotProductI7u(MemorySegment a, MemorySegment b, int length) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, length, (int) a.byteSize());
            return callSingleDistanceInt(dotI7uHandle, a, b, length);
        }

        private static final MethodHandle squareI7uHandle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT7U, Operation.SINGLE)
        );

        static int squareDistanceI7u(MemorySegment a, MemorySegment b, int length) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, length, (int) a.byteSize());
            return callSingleDistanceInt(squareI7uHandle, a, b, length);
        }

        private static final MethodHandle cosI8Handle = HANDLES.get(
            new OperationSignature<>(Function.COSINE, DataType.INT8, Operation.SINGLE)
        );

        static float cosineI8(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, elementCount, (int) a.byteSize());
            return callSingleDistanceFloat(cosI8Handle, a, b, elementCount);
        }

        private static final MethodHandle dotI8Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT8, Operation.SINGLE)
        );

        static float dotProductI8(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, elementCount, (int) a.byteSize());
            return callSingleDistanceFloat(dotI8Handle, a, b, elementCount);
        }

        private static final MethodHandle squareI8Handle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT8, Operation.SINGLE)
        );

        static float squareDistanceI8(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, elementCount, (int) a.byteSize());
            return callSingleDistanceFloat(squareI8Handle, a, b, elementCount);
        }

        private static final MethodHandle dotF32Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.FLOAT32, Operation.SINGLE)
        );

        static float dotProductF32(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, elementCount, (int) a.byteSize() / Float.BYTES);
            return callSingleDistanceFloat(dotF32Handle, a, b, elementCount);
        }

        private static final MethodHandle squareF32Handle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.FLOAT32, Operation.SINGLE)
        );

        static float squareDistanceF32(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, elementCount, (int) a.byteSize() / Float.BYTES);
            return callSingleDistanceFloat(squareF32Handle, a, b, elementCount);
        }

        private static final MethodHandle dotD1Q4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D1Q4, Operation.SINGLE)
        );

        static long dotProductD1Q4(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0, length * 4L, (int) query.byteSize());
            Objects.checkFromIndexSize(0, length, (int) a.byteSize());
            return callSingleDistanceLong(dotD1Q4Handle, a, query, length);
        }

        private static final MethodHandle dotD2Q4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q4, Operation.SINGLE)
        );

        static long dotProductD2Q4(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0, length * 2, (int) query.byteSize());
            Objects.checkFromIndexSize(0, length, (int) a.byteSize());
            return callSingleDistanceLong(dotD2Q4Handle, a, query, length);
        }

        private static final MethodHandle dotD4Q4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D4Q4, Operation.SINGLE)
        );

        static long dotProductD4Q4(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0, length, (int) query.byteSize());
            Objects.checkFromIndexSize(0, length, (int) a.byteSize());
            return callSingleDistanceLong(dotD4Q4Handle, a, query, length);
        }

        private static void checkByteSize(MemorySegment a, MemorySegment b) {
            if (a.byteSize() != b.byteSize()) {
                throw new IllegalArgumentException("Dimensions differ: " + a.byteSize() + "!=" + b.byteSize());
            }
        }

        private static float applyCorrectionsEuclideanBulk(
            MemorySegment corrections,
            int bulkSize,
            int dimensions,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            float queryBitScale,
            float indexBitScale,
            float centroidDp,
            MemorySegment scores
        ) {
            try {
                return (float) applyCorrectionsEuclideanBulk$mh.invokeExact(
                    corrections,
                    bulkSize,
                    dimensions,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    queryBitScale,
                    indexBitScale,
                    centroidDp,
                    scores
                );
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static float applyCorrectionsMaxInnerProductBulk(
            MemorySegment corrections,
            int bulkSize,
            int dimensions,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            float queryBitScale,
            float indexBitScale,
            float centroidDp,
            MemorySegment scores
        ) {
            try {
                return (float) applyCorrectionsMaxInnerProductBulk$mh.invokeExact(
                    corrections,
                    bulkSize,
                    dimensions,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    queryBitScale,
                    indexBitScale,
                    centroidDp,
                    scores
                );
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static float applyCorrectionsDotProductBulk(
            MemorySegment corrections,
            int bulkSize,
            int dimensions,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            float queryBitScale,
            float indexBitScale,
            float centroidDp,
            MemorySegment scores
        ) {
            try {
                return (float) applyCorrectionsDotProductBulk$mh.invokeExact(
                    corrections,
                    bulkSize,
                    dimensions,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    queryBitScale,
                    indexBitScale,
                    centroidDp,
                    scores
                );
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static final Map<OperationSignature<?>, MethodHandle> HANDLES_WITH_CHECKS;

        static final MethodHandle APPLY_CORRECTIONS_EUCLIDEAN_HANDLE_BULK;
        static final MethodHandle APPLY_CORRECTIONS_MAX_INNER_PRODUCT_HANDLE_BULK;
        static final MethodHandle APPLY_CORRECTIONS_DOT_PRODUCT_HANDLE_BULK;

        static {
            MethodHandles.Lookup lookup = MethodHandles.lookup();

            try {
                Map<OperationSignature<?>, MethodHandle> handlesWithChecks = new HashMap<>();

                for (var op : HANDLES.entrySet()) {
                    switch (op.getKey().operation()) {
                        case SINGLE -> {
                            // Single score methods are called once for each vector,
                            // this means we need to reduce the overheads as much as possible.
                            // So have specific hard-coded check methods rather than use guardWithTest
                            // to create the check-and-call methods dynamically
                            String checkMethod = switch (op.getKey().function()) {
                                case COSINE -> "cosine";
                                case DOT_PRODUCT -> "dotProduct";
                                case SQUARE_DISTANCE -> "squareDistance";
                            };

                            MethodHandle handleWithChecks = switch (op.getKey().dataType()) {
                                case DataType dt -> {
                                    MethodType type = null;

                                    switch (dt) {
                                        case INT7U:
                                            type = MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class, int.class);
                                            checkMethod += "I7u";
                                            break;
                                        case INT8:
                                            type = MethodType.methodType(float.class, MemorySegment.class, MemorySegment.class, int.class);
                                            checkMethod += "I8";
                                            break;
                                        case FLOAT32:
                                            type = MethodType.methodType(float.class, MemorySegment.class, MemorySegment.class, int.class);
                                            checkMethod += "F32";
                                            break;
                                    }
                                    yield lookup.findStatic(JdkVectorSimilarityFunctions.class, checkMethod, type);
                                }
                                case BBQType bbq -> {
                                    MethodType type = MethodType.methodType(
                                        long.class,
                                        MemorySegment.class,
                                        MemorySegment.class,
                                        int.class
                                    );
                                    yield lookup.findStatic(JdkVectorSimilarityFunctions.class, checkMethod + bbq, type);
                                }
                                default -> throw new IllegalArgumentException("Unknown handle type " + op.getKey().dataType());
                            };

                            handlesWithChecks.put(op.getKey(), handleWithChecks);
                        }
                        case BULK -> {
                            MethodHandle handleWithChecks = switch (op.getKey().dataType()) {
                                case BBQType bbq -> {
                                    MethodHandle checkMethod = lookup.findStatic(
                                        JdkVectorSimilarityFunctions.class,
                                        "checkBBQBulk",
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
                                        MethodHandles.insertArguments(checkMethod, 0, bbq.dataBits()),
                                        op.getValue(),
                                        MethodHandles.empty(op.getValue().type())
                                    );
                                }
                                case DataType dt -> {
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
                                        MethodHandles.insertArguments(checkMethod, 0, dt.bytes()),
                                        op.getValue(),
                                        MethodHandles.empty(op.getValue().type())
                                    );
                                }
                                default -> throw new IllegalArgumentException("Unknown handle type " + op.getKey().dataType());
                            };

                            handlesWithChecks.put(op.getKey(), handleWithChecks);
                        }
                        case BULK_OFFSETS -> {
                            MethodHandle handleWithChecks = switch (op.getKey().dataType()) {
                                case BBQType _ -> {
                                    MethodHandle checkMethod = lookup.findStatic(
                                        JdkVectorSimilarityFunctions.class,
                                        "checkBBQBulkOffsets",
                                        MethodType.methodType(
                                            boolean.class,
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
                                        checkMethod,
                                        op.getValue(),
                                        MethodHandles.empty(op.getValue().type())
                                    );
                                }
                                case DataType dt -> {
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
                                        MethodHandles.insertArguments(checkMethod, 0, dt.bytes()),
                                        op.getValue(),
                                        MethodHandles.empty(op.getValue().type())
                                    );
                                }
                                default -> throw new IllegalArgumentException("Unknown handle type " + op.getKey().dataType());
                            };

                            handlesWithChecks.put(op.getKey(), handleWithChecks);
                        }
                    }
                }

                HANDLES_WITH_CHECKS = Collections.unmodifiableMap(handlesWithChecks);

                MethodType scoringFunction = MethodType.methodType(
                    float.class,
                    MemorySegment.class,
                    int.class,
                    int.class,
                    float.class,
                    float.class,
                    int.class,
                    float.class,
                    float.class,
                    float.class,
                    float.class,
                    MemorySegment.class
                );

                APPLY_CORRECTIONS_EUCLIDEAN_HANDLE_BULK = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "applyCorrectionsEuclideanBulk",
                    scoringFunction
                );
                APPLY_CORRECTIONS_MAX_INNER_PRODUCT_HANDLE_BULK = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "applyCorrectionsMaxInnerProductBulk",
                    scoringFunction
                );
                APPLY_CORRECTIONS_DOT_PRODUCT_HANDLE_BULK = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "applyCorrectionsDotProductBulk",
                    scoringFunction
                );
            } catch (ReflectiveOperationException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public MethodHandle getHandle(Function function, DataType dataType, Operation operation) {
            OperationSignature<?> key = new OperationSignature<>(function, dataType, operation);
            MethodHandle mh = HANDLES_WITH_CHECKS.get(key);
            if (mh == null) throw new IllegalArgumentException("Signature not implemented: " + key);
            return mh;
        }

        @Override
        public MethodHandle getHandle(Function function, BBQType bbqType, Operation operation) {
            OperationSignature<?> key = new OperationSignature<>(function, bbqType, operation);
            MethodHandle mh = HANDLES_WITH_CHECKS.get(key);
            if (mh == null) throw new IllegalArgumentException("Signature not implemented: " + key);
            return mh;
        }

        @Override
        public MethodHandle applyCorrectionsEuclideanBulk() {
            return APPLY_CORRECTIONS_EUCLIDEAN_HANDLE_BULK;
        }

        @Override
        public MethodHandle applyCorrectionsMaxInnerProductBulk() {
            return APPLY_CORRECTIONS_MAX_INNER_PRODUCT_HANDLE_BULK;
        }

        @Override
        public MethodHandle applyCorrectionsDotProductBulk() {
            return APPLY_CORRECTIONS_DOT_PRODUCT_HANDLE_BULK;
        }
    }
}
