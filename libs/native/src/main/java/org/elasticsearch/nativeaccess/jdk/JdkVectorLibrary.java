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
import org.elasticsearch.foreign.LoaderHelper;
import org.elasticsearch.foreign.adapter.LinkerAdapter;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.BBQType;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.BFloat16QueryType;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.DataType;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Function;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Operation;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.foreign.LinkerHelper.downcallHandle;
import static org.elasticsearch.foreign.LinkerHelper.functionAddressOrNull;

public final class JdkVectorLibrary implements VectorLibrary {

    private static final Logger logger = LogManager.getLogger(JdkVectorLibrary.class);
    private static final int VEC_CAPS_OVERRIDE = getVecCapsOverride();

    private record OperationSignature<E extends Enum<E>>(Function function, E dataType, Operation operation) {}

    private static final Map<OperationSignature<?>, MethodHandle> HANDLES;

    static final MethodHandle applyCorrectionsEuclideanBulk$mh;
    static final MethodHandle applyCorrectionsMaxInnerProductBulk$mh;
    static final MethodHandle applyCorrectionsDotProductBulk$mh;

    static final MethodHandle bbqApplyCorrectionsEuclideanBulk$mh;
    static final MethodHandle bbqApplyCorrectionsMaxInnerProductBulk$mh;
    static final MethodHandle bbqApplyCorrectionsDotProductBulk$mh;

    private static final JdkVectorSimilarityFunctions INSTANCE;

    /**
     * Try to get a vec_caps override value from the {@code es.vec_caps_override} system property.
     * This value is used to override the vector capabilities value returned by the native call
     * to {@code vec_caps}; if the override is defined and valid (>= 0), and it is less then the
     * one returned by {@code vec_caps}, the override is used to determine which functions to bind.
     * This can be used to force binding to functions from a lower tier (e.g. AVX2 on a AVX-512
     * capable processor), or to disable native functions completely (by passing 0).
     * Usage: {@code -Des.vec_caps_override=1}.
     * For benchmarks, add {@code --jvmArgsPrepend "--add-modules=jdk.incubator.vector -Des.vec_caps_override=..."}
     * to {@code --args}. Note: {@code --jvmArgsPrepend} on the CLI replaces the {@code @Fork} annotation's
     * {@code jvmArgsPrepend}, so {@code --add-modules=jdk.incubator.vector} must be included explicitly.
     *
     * @return the caps override value, or -1 if the property is not defined or invalid.
     */
    private static int getVecCapsOverride() {
        try {
            var capsOverrideString = System.getProperty("es.vec_caps_override", "-1");
            try {
                return Integer.parseInt(capsOverrideString);
            } catch (NumberFormatException e) {
                logger.warn("Invalid es.vec_caps_override value [{}]", capsOverrideString);
                return -1;
            }
        } catch (Throwable t) {
            logger.warn("Cannot read es.vec_caps_override value", t);
        }
        return -1;
    }

    /**
     * Native functions in the native simdvec library can have multiple implementations, one for each "capability level".
     * A capability level of "0" means that there is no native function for that platform.
     * Functions for the base ("1") level are exposed with a simple function name (e.g. "vec_doti7u")
     * Functions for the more advanced levels (2, 3, ...) are exported with a name "decorated" by adding the capability level as
     * a suffix: if the capability level is N, the suffix will be "_N" (e.g. "vec_doti7u_2").
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
                return downcallHandle(function, functionDescriptor, LinkerAdapter.critical());
            }
        }
        throw new LinkageError("Native function [" + functionName + "] could not be found");
    }

    private static class NativeFunctions {

        private record NativeFunction<E extends Enum<E>>(OperationSignature<E> op, String typeName, FunctionDescriptor descriptor) {}

        private static final FunctionDescriptor intSingle = FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT);
        private static final FunctionDescriptor longSingle = FunctionDescriptor.of(JAVA_LONG, ADDRESS, ADDRESS, JAVA_INT);
        private static final FunctionDescriptor floatSingle = FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT);
        private static final FunctionDescriptor bulk = FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS);
        private static final FunctionDescriptor bulkOffsets = FunctionDescriptor.ofVoid(
            ADDRESS,
            ADDRESS,
            JAVA_INT,
            JAVA_INT,
            ADDRESS,
            JAVA_INT,
            ADDRESS
        );
        final List<NativeFunction<?>> nativeFunctions = new ArrayList<>();

        public NativeFunctions add(DataType type, Iterable<Function> functions, Iterable<Operation> operations) {
            functions.forEach(f -> operations.forEach(op -> {
                String typeName = switch (type) {
                    case INT7U -> "i7u";
                    case INT4 -> "i4";
                    case INT8 -> "i8";
                    case FLOAT32 -> "f32";
                };
                FunctionDescriptor descriptor = switch (op) {
                    case SINGLE -> switch (type) {
                        case INT7U, INT4 -> intSingle;
                        case INT8, FLOAT32 -> floatSingle;
                    };
                    case BULK, BULK_SPARSE -> bulk;
                    case BULK_OFFSETS -> bulkOffsets;
                };
                nativeFunctions.add(new NativeFunction<>(new OperationSignature<>(f, type, op), typeName, descriptor));
            }));
            return this;
        }

        public NativeFunctions addBFloat16(Iterable<Function> functions, Iterable<Operation> operations) {
            for (BFloat16QueryType type : BFloat16QueryType.values()) {
                String typeName = switch (type) {
                    case BFLOAT16 -> "Dbf16Qbf16";
                    case FLOAT32 -> "Dbf16Qf32";
                };
                functions.forEach(f -> operations.forEach(op -> {
                    FunctionDescriptor descriptor = switch (op) {
                        case SINGLE -> floatSingle;
                        case BULK, BULK_SPARSE -> bulk;
                        case BULK_OFFSETS -> bulkOffsets;
                    };
                    nativeFunctions.add(new NativeFunction<>(new OperationSignature<>(f, type, op), typeName, descriptor));
                }));
            }
            return this;
        }

        public NativeFunctions addBBQ(Iterable<BBQType> bbqTypes, Iterable<Operation> operations) {
            bbqTypes.forEach(type -> operations.forEach(op -> {
                String typeName = switch (type) {
                    case D1Q1 -> "d1q1";
                    case D1Q4 -> "d1q4";
                    case D2Q2 -> "d2q2";
                    case D2Q4 -> "d2q4";
                    case D4Q4 -> "d4q4";
                    case D2Q4_PACKED -> "d2q4_packed";
                };
                FunctionDescriptor descriptor = switch (op) {
                    case SINGLE -> longSingle;
                    case BULK, BULK_SPARSE -> bulk;
                    case BULK_OFFSETS -> bulkOffsets;
                };
                nativeFunctions.add(new NativeFunction<>(new OperationSignature<>(Function.DOT_PRODUCT, type, op), typeName, descriptor));
            }));
            return this;
        }

        static Iterable<Function> allFunctions() {
            return () -> Arrays.stream(Function.values()).iterator();
        }

        static Iterable<Operation> standardOperations() {
            return List.of(Operation.SINGLE, Operation.BULK, Operation.BULK_OFFSETS, Operation.BULK_SPARSE);
        }

        static Iterable<BBQType> allBBQTypes() {
            return () -> Arrays.stream(BBQType.values()).iterator();
        }

        private static String getOpName(Operation op) {
            return switch (op) {
                case SINGLE -> "";
                case BULK -> "_bulk";
                case BULK_OFFSETS -> "_bulk_offsets";
                case BULK_SPARSE -> "_bulk_sparse";
            };
        }

        private static String getFuncName(Function f) {
            return switch (f) {
                case COSINE -> "cos";
                case DOT_PRODUCT -> "dot";
                case SQUARE_DISTANCE -> "sqr";
            };
        }

        public Map<OperationSignature<?>, MethodHandle> build(BiFunction<String, FunctionDescriptor, MethodHandle> binder) {
            return nativeFunctions.stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        f -> f.op,
                        f -> binder.apply("vec_" + getFuncName(f.op.function()) + f.typeName + getOpName(f.op.operation()), f.descriptor)
                    )
                );
        }
    }

    static {
        LoaderHelper.loadLibrary("vec");
        MethodHandle vecCaps$mh = downcallHandle("vec_caps", FunctionDescriptor.of(JAVA_INT));

        try {
            int vecCaps = (int) vecCaps$mh.invokeExact();
            final int finalVecCaps;
            if (VEC_CAPS_OVERRIDE >= 0) {
                finalVecCaps = Math.min(vecCaps, VEC_CAPS_OVERRIDE);
                logger.info("vec_caps={}; es.vec_caps_override={}; using [{}]", vecCaps, VEC_CAPS_OVERRIDE, finalVecCaps);
            } else {
                finalVecCaps = vecCaps;
                logger.info("vec_caps=" + finalVecCaps);
            }

            if (finalVecCaps > 0) {

                HANDLES = new NativeFunctions()
                    // Only DOT_PRODUCT is needed for int4 — other functions are computed by applying correction terms on top of the raw
                    // dot.
                    .add(DataType.INT4, List.of(Function.DOT_PRODUCT), NativeFunctions.standardOperations())
                    .add(DataType.INT7U, List.of(Function.DOT_PRODUCT, Function.SQUARE_DISTANCE), NativeFunctions.standardOperations())
                    // Only byte vectors have cosine as other types are normalized to unit length to use dot_product instead
                    .add(DataType.INT8, NativeFunctions.allFunctions(), NativeFunctions.standardOperations())
                    .add(DataType.FLOAT32, List.of(Function.DOT_PRODUCT, Function.SQUARE_DISTANCE), NativeFunctions.standardOperations())
                    .addBFloat16(List.of(Function.DOT_PRODUCT, Function.SQUARE_DISTANCE), NativeFunctions.standardOperations())
                    .addBBQ(NativeFunctions.allBBQTypes(), NativeFunctions.standardOperations())
                    .build((functionName, functionDescriptor) -> bindFunction(functionName, finalVecCaps, functionDescriptor));

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

                applyCorrectionsEuclideanBulk$mh = bindFunction("diskbbq_apply_corrections_euclidean_bulk", finalVecCaps, score);
                applyCorrectionsMaxInnerProductBulk$mh = bindFunction(
                    "diskbbq_apply_corrections_maximum_inner_product_bulk",
                    finalVecCaps,
                    score
                );
                applyCorrectionsDotProductBulk$mh = bindFunction("diskbbq_apply_corrections_dot_product_bulk", finalVecCaps, score);

                // BBQ corrections: per-vector inline layout with nodes array
                FunctionDescriptor bbqScore = FunctionDescriptor.of(
                    JAVA_FLOAT,
                    ADDRESS,     // data (entire vector data segment)
                    JAVA_INT,    // bulkSize
                    JAVA_INT,    // vectorSizeInBytes
                    JAVA_INT,    // pitchInBytes
                    JAVA_INT,    // dimensions
                    JAVA_FLOAT,  // queryLowerInterval
                    JAVA_FLOAT,  // queryUpperInterval
                    JAVA_INT,    // queryComponentSum
                    JAVA_FLOAT,  // queryAdditionalCorrection
                    JAVA_FLOAT,  // queryBitScale
                    JAVA_FLOAT,  // indexBitScale
                    JAVA_FLOAT,  // centroidDp
                    JAVA_BYTE,   // readComponentSumAsInt (0 = 2-byte format, 1 = 4-byte format)
                    ADDRESS      // scores
                );

                bbqApplyCorrectionsEuclideanBulk$mh = bindFunction("bbq_apply_corrections_euclidean_bulk", finalVecCaps, bbqScore);
                bbqApplyCorrectionsMaxInnerProductBulk$mh = bindFunction(
                    "bbq_apply_corrections_maximum_inner_product_bulk",
                    finalVecCaps,
                    bbqScore
                );
                bbqApplyCorrectionsDotProductBulk$mh = bindFunction("bbq_apply_corrections_dot_product_bulk", finalVecCaps, bbqScore);

                INSTANCE = new JdkVectorSimilarityFunctions();
            } else {
                if (finalVecCaps < 0) {
                    logger.warn("""
                        Your CPU supports vector capabilities, but they are disabled at OS level. For optimal performance, \
                        enable them in your OS/Hypervisor/VM/container""");
                }
                HANDLES = null;
                applyCorrectionsEuclideanBulk$mh = null;
                applyCorrectionsMaxInnerProductBulk$mh = null;
                applyCorrectionsDotProductBulk$mh = null;
                bbqApplyCorrectionsEuclideanBulk$mh = null;
                bbqApplyCorrectionsMaxInnerProductBulk$mh = null;
                bbqApplyCorrectionsDotProductBulk$mh = null;
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

        static boolean checkBulk(int elementBits, MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            Objects.checkFromIndexSize(0L, (long) length * count * elementBits / 8, a.byteSize());
            Objects.checkFromIndexSize(0L, length, b.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
            return true;
        }

        static boolean checkBFloat16Bulk(
            int queryElementSize,
            MemorySegment a,
            MemorySegment b,
            int length,
            int count,
            MemorySegment result
        ) {
            Objects.checkFromIndexSize(0L, (long) length * count * Short.BYTES, a.byteSize());
            Objects.checkFromIndexSize(0L, (long) length * queryElementSize, b.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
            return true;
        }

        /**
         * Checks dimensions for BBQ bulk operations.
         * @param queryBytesPerDocByte see {@link BBQType#queryBytesPerDocByte()}
         * @param dataset the MemorySegment holding the vectors to score
         * @param query the MemorySegment holding the query vector
         * @param datasetVectorLengthInBytes
         * @param count
         * @param result the MemorySegment holding the result scores
         */
        static boolean checkBBQBulk(
            int queryBytesPerDocByte,
            MemorySegment dataset,
            MemorySegment query,
            int datasetVectorLengthInBytes,
            int count,
            MemorySegment result
        ) {
            Objects.checkFromIndexSize(0L, (long) datasetVectorLengthInBytes * count, dataset.byteSize());
            Objects.checkFromIndexSize(0L, (long) datasetVectorLengthInBytes * queryBytesPerDocByte, query.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
            return true;
        }

        static boolean checkBulkSparse(
            int elementBits,
            MemorySegment addresses,
            MemorySegment b,
            int length,
            int count,
            MemorySegment result
        ) {
            long queryBytes = elementBits >= 8 ? (long) length * elementBits / 8 : length;
            Objects.checkFromIndexSize(0L, (long) count * Long.BYTES, addresses.byteSize());
            Objects.checkFromIndexSize(0L, queryBytes, b.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
            assert validateBulkSparse(addresses, count, length, elementBits, result);
            return true;
        }

        static boolean checkBulkOffsets(
            int elementBits,
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            long rowBytes = (long) length * elementBits / 8;
            if (pitch < rowBytes) throw new IllegalArgumentException("Pitch needs to be at least " + rowBytes);
            Objects.checkFromIndexSize(0L, (long) pitch * count, a.byteSize());
            Objects.checkFromIndexSize(0L, rowBytes, b.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Integer.BYTES, offsets.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
            assert validateBulkOffsets(a, offsets, count, length, pitch, result, rowBytes);
            return true;
        }

        static boolean checkBFloat16BulkOffsets(
            int queryElementSize,
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            if (pitch < length * Short.BYTES) throw new IllegalArgumentException("Pitch needs to be at least " + length * Short.BYTES);
            Objects.checkFromIndexSize(0L, (long) pitch * count, a.byteSize());
            Objects.checkFromIndexSize(0L, (long) length * queryElementSize, b.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Integer.BYTES, offsets.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
            assert validateBulkOffsets(a, offsets, count, length, pitch, result, (long) length * Short.BYTES);
            return true;
        }

        static boolean checkBFloat16BulkSparse(
            int queryElementSize,
            MemorySegment addresses,
            MemorySegment b,
            int length,
            int count,
            MemorySegment result
        ) {
            Objects.checkFromIndexSize(0L, (long) count * Long.BYTES, addresses.byteSize());
            Objects.checkFromIndexSize(0L, (long) length * queryElementSize, b.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
            assert validateBulkSparse(addresses, count, length, 16, result);
            return true;
        }

        static boolean checkBBQBulkSparse(
            int queryBytesPerDocByte,
            MemorySegment addresses,
            MemorySegment query,
            int datasetVectorLengthInBytes,
            int count,
            MemorySegment result
        ) {
            Objects.checkFromIndexSize(0L, (long) count * Long.BYTES, addresses.byteSize());
            Objects.checkFromIndexSize(0L, (long) datasetVectorLengthInBytes * queryBytesPerDocByte, query.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
            return true;
        }

        static boolean checkBBQBulkOffsets(
            int queryBytesPerDocByte,
            MemorySegment a,
            MemorySegment b,
            int datasetVectorLengthInBytes,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            if (pitch < datasetVectorLengthInBytes) throw new IllegalArgumentException(
                "Pitch needs to be at least " + datasetVectorLengthInBytes
            );
            Objects.checkFromIndexSize(0L, (long) datasetVectorLengthInBytes * count, a.byteSize());
            // STRIPED / PACKED: see BBQType#queryBytesPerDocByte for the layout-dependent multiplier.
            Objects.checkFromIndexSize(0L, (long) datasetVectorLengthInBytes * queryBytesPerDocByte, b.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Integer.BYTES, offsets.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
            assert validateBBQBulkOffsets(a, offsets, count, datasetVectorLengthInBytes, pitch, result);
            return true;
        }

        static boolean validateBulkOffsets(
            MemorySegment a,
            MemorySegment offsets,
            int count,
            int length,
            int pitch,
            MemorySegment result,
            long rowBytes
        ) {
            if (count < 0) throw new IllegalArgumentException("count must be non-negative: " + count);
            if (length <= 0) throw new IllegalArgumentException("length must be positive: " + length);
            if (pitch <= 0) throw new IllegalArgumentException("pitch must be positive: " + pitch);
            checkSegmentAlignment(offsets, Integer.BYTES, "offsets", "int");
            checkSegmentAlignment(result, Float.BYTES, "result", "float");
            long aSize = a.byteSize();
            for (int i = 0; i < count; i++) {
                int offset = offsets.getAtIndex(JAVA_INT, i);
                Objects.checkFromIndexSize((long) offset * pitch, rowBytes, aSize);
            }
            return true;
        }

        static boolean validateBBQBulkOffsets(
            MemorySegment a,
            MemorySegment offsets,
            int count,
            int datasetVectorLengthInBytes,
            int pitch,
            MemorySegment result
        ) {
            if (count < 0) throw new IllegalArgumentException("count must be non-negative: " + count);
            if (datasetVectorLengthInBytes <= 0) {
                throw new IllegalArgumentException("datasetVectorLengthInBytes must be positive: " + datasetVectorLengthInBytes);
            }
            if (pitch <= 0) throw new IllegalArgumentException("pitch must be positive: " + pitch);
            checkSegmentAlignment(offsets, Integer.BYTES, "offsets", "int");
            checkSegmentAlignment(result, Float.BYTES, "result", "float");
            long aSize = a.byteSize();
            for (int i = 0; i < count; i++) {
                int offset = offsets.getAtIndex(JAVA_INT, i);
                Objects.checkFromIndexSize((long) offset * pitch, datasetVectorLengthInBytes, aSize);
            }
            return true;
        }

        static boolean validateBulkSparse(MemorySegment addresses, int count, int length, int elementBits, MemorySegment result) {
            if (count < 0) throw new IllegalArgumentException("count must be non-negative: " + count);
            if (length <= 0) throw new IllegalArgumentException("length must be positive: " + length);
            checkSegmentAlignment(addresses, Long.BYTES, "addresses", "long");
            checkSegmentAlignment(result, Float.BYTES, "result", "float");
            long vectorBytes = elementBits >= 8 ? (long) length * elementBits / 8 : length;
            for (int i = 0; i < count; i++) {
                long addr = addresses.getAtIndex(JAVA_LONG, i);
                if (addr == 0) {
                    throw new IllegalArgumentException("address at index " + i + " is null");
                }
                MemorySegment vec = MemorySegment.ofAddress(addr).reinterpret(vectorBytes);
                Objects.checkFromIndexSize(0L, vectorBytes, vec.byteSize());
            }
            return true;
        }

        private static void checkSegmentAlignment(MemorySegment segment, int alignment, String name, String type) {
            if (segment.address() % alignment != 0) {
                throw new IllegalArgumentException(name + " segment not aligned to " + type + " boundary");
            }
        }

        private static final MethodHandle dotI7uHandle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT7U, Operation.SINGLE)
        );

        static int dotProductI7uChecked(MemorySegment a, MemorySegment b, int length) {
            checkByteSize(a.byteSize(), b.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceInt(dotI7uHandle, a, b, length);
        }

        private static final MethodHandle squareI7uHandle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT7U, Operation.SINGLE)
        );

        static int squareDistanceI7uChecked(MemorySegment a, MemorySegment b, int length) {
            checkByteSize(a.byteSize(), b.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceInt(squareI7uHandle, a, b, length);
        }

        private static final MethodHandle dotI4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT4, Operation.SINGLE)
        );

        static int dotProductI4Checked(MemorySegment a, MemorySegment b, int elementCount) {
            Objects.checkFromIndexSize(0L, 2L * elementCount, a.byteSize());
            Objects.checkFromIndexSize(0L, elementCount, b.byteSize());
            return callSingleDistanceInt(dotI4Handle, a, b, elementCount);
        }

        private static final MethodHandle cosI8Handle = HANDLES.get(
            new OperationSignature<>(Function.COSINE, DataType.INT8, Operation.SINGLE)
        );

        static float cosineI8Checked(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a.byteSize(), b.byteSize());
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize());
            return callSingleDistanceFloat(cosI8Handle, a, b, elementCount);
        }

        private static final MethodHandle dotI8Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT8, Operation.SINGLE)
        );

        static float dotProductI8Checked(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a.byteSize(), b.byteSize());
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize());
            return callSingleDistanceFloat(dotI8Handle, a, b, elementCount);
        }

        private static final MethodHandle squareI8Handle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT8, Operation.SINGLE)
        );

        static float squareDistanceI8Checked(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a.byteSize(), b.byteSize());
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize());
            return callSingleDistanceFloat(squareI8Handle, a, b, elementCount);
        }

        private static final MethodHandle dotF32Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.FLOAT32, Operation.SINGLE)
        );

        static float dotProductF32Checked(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a.byteSize(), b.byteSize());
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize() / Float.BYTES);
            return callSingleDistanceFloat(dotF32Handle, a, b, elementCount);
        }

        private static final MethodHandle squareF32Handle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.FLOAT32, Operation.SINGLE)
        );

        static float squareDistanceF32Checked(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a.byteSize(), b.byteSize());
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize() / Float.BYTES);
            return callSingleDistanceFloat(squareF32Handle, a, b, elementCount);
        }

        private static final MethodHandle dotDBF16QF32Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BFloat16QueryType.FLOAT32, Operation.SINGLE)
        );

        static float dotProductDBF16QF32Checked(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a.byteSize(), b.byteSize() / 2);
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize() / Short.BYTES);
            Objects.checkFromIndexSize(0L, elementCount, b.byteSize() / Float.BYTES);
            return callSingleDistanceFloat(dotDBF16QF32Handle, a, b, elementCount);
        }

        private static final MethodHandle dotDBF16QBF16Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BFloat16QueryType.BFLOAT16, Operation.SINGLE)
        );

        static float dotProductDBF16QBF16Checked(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a.byteSize(), b.byteSize());
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize() / Short.BYTES);
            Objects.checkFromIndexSize(0L, elementCount, b.byteSize() / Short.BYTES);
            return callSingleDistanceFloat(dotDBF16QBF16Handle, a, b, elementCount);
        }

        private static final MethodHandle squareDBF16QF32Handle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, BFloat16QueryType.FLOAT32, Operation.SINGLE)
        );

        static float squareDistanceDBF16QF32Checked(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a.byteSize(), b.byteSize() / 2);
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize() / Short.BYTES);
            Objects.checkFromIndexSize(0L, elementCount, b.byteSize() / Float.BYTES);
            return callSingleDistanceFloat(squareDBF16QF32Handle, a, b, elementCount);
        }

        private static final MethodHandle squareDBF16QBF16Handle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, BFloat16QueryType.BFLOAT16, Operation.SINGLE)
        );

        static float squareDistanceDBF16QBF16Checked(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a.byteSize(), b.byteSize());
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize() / Short.BYTES);
            Objects.checkFromIndexSize(0L, elementCount, b.byteSize() / Short.BYTES);
            return callSingleDistanceFloat(squareDBF16QBF16Handle, a, b, elementCount);
        }

        private static final MethodHandle dotD1Q1Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D1Q1, Operation.SINGLE)
        );

        static long dotProductD1Q1Checked(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0L, length, query.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceLong(dotD1Q1Handle, a, query, length);
        }

        private static final MethodHandle dotD1Q4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D1Q4, Operation.SINGLE)
        );

        static long dotProductD1Q4Checked(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0L, (long) length * 4, query.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceLong(dotD1Q4Handle, a, query, length);
        }

        private static final MethodHandle dotD2Q2Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q2, Operation.SINGLE)
        );

        static long dotProductD2Q2Checked(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0L, length, query.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceLong(dotD2Q2Handle, a, query, length);
        }

        private static final MethodHandle dotD2Q4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q4, Operation.SINGLE)
        );

        static long dotProductD2Q4Checked(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0L, (long) length * 2, query.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceLong(dotD2Q4Handle, a, query, length);
        }

        private static final MethodHandle dotD4Q4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D4Q4, Operation.SINGLE)
        );

        static long dotProductD4Q4Checked(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0L, length, query.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceLong(dotD4Q4Handle, a, query, length);
        }

        private static final MethodHandle dotD2Q4PackedHandle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q4_PACKED, Operation.SINGLE)
        );

        static long dotProductD2Q4_PACKEDChecked(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0L, (long) length * 4, query.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceLong(dotD2Q4PackedHandle, a, query, length);
        }

        private static void checkByteSize(long aSize, long bSize) {
            if (aSize != bSize) {
                throw new IllegalArgumentException("Dimensions differ: " + aSize + "!=" + bSize);
            }
        }

        private static final MethodHandle dotProductI7uBulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT7U, Operation.BULK)
        );
        private static final MethodHandle squareDistanceI7uBulk$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT7U, Operation.BULK)
        );
        private static final MethodHandle dotProductI4Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT4, Operation.BULK)
        );
        private static final MethodHandle cosineI8Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.COSINE, DataType.INT8, Operation.BULK)
        );
        private static final MethodHandle dotProductI8Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT8, Operation.BULK)
        );
        private static final MethodHandle squareDistanceI8Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT8, Operation.BULK)
        );
        private static final MethodHandle dotProductF32Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.FLOAT32, Operation.BULK)
        );
        private static final MethodHandle squareDistanceF32Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.FLOAT32, Operation.BULK)
        );
        private static final MethodHandle dotProductDBF16QF32Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BFloat16QueryType.FLOAT32, Operation.BULK)
        );
        private static final MethodHandle squareDistanceDBF16QF32Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, BFloat16QueryType.FLOAT32, Operation.BULK)
        );
        private static final MethodHandle dotProductDBF16QBF16Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BFloat16QueryType.BFLOAT16, Operation.BULK)
        );
        private static final MethodHandle squareDistanceDBF16QBF16Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, BFloat16QueryType.BFLOAT16, Operation.BULK)
        );
        private static final MethodHandle dotProductD1Q1Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D1Q1, Operation.BULK)
        );
        private static final MethodHandle dotProductD1Q4Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D1Q4, Operation.BULK)
        );
        private static final MethodHandle dotProductD2Q2Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q2, Operation.BULK)
        );
        private static final MethodHandle dotProductD2Q4Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q4, Operation.BULK)
        );
        private static final MethodHandle dotProductD4Q4Bulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D4Q4, Operation.BULK)
        );
        private static final MethodHandle dotProductD2Q4PackedBulk$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q4_PACKED, Operation.BULK)
        );

        private static final MethodHandle dotProductI7uBulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT7U, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle squareDistanceI7uBulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT7U, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductI4BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT4, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle cosineI8BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.COSINE, DataType.INT8, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductI8BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT8, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle squareDistanceI8BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT8, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductF32BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.FLOAT32, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle squareDistanceF32BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.FLOAT32, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductDBF16QF32BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BFloat16QueryType.FLOAT32, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle squareDistanceDBF16QF32BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, BFloat16QueryType.FLOAT32, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductDBF16QBF16BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BFloat16QueryType.BFLOAT16, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle squareDistanceDBF16QBF16BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, BFloat16QueryType.BFLOAT16, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductD1Q1BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D1Q1, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductD1Q4BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D1Q4, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductD2Q2BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q2, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductD2Q4BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q4, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductD4Q4BulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D4Q4, Operation.BULK_OFFSETS)
        );
        private static final MethodHandle dotProductD2Q4PackedBulkWithOffsets$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q4_PACKED, Operation.BULK_OFFSETS)
        );

        private static final MethodHandle dotProductI7uBulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT7U, Operation.BULK_SPARSE)
        );
        private static final MethodHandle squareDistanceI7uBulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT7U, Operation.BULK_SPARSE)
        );
        private static final MethodHandle dotProductI4BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT4, Operation.BULK_SPARSE)
        );
        private static final MethodHandle cosineI8BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.COSINE, DataType.INT8, Operation.BULK_SPARSE)
        );
        private static final MethodHandle dotProductI8BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT8, Operation.BULK_SPARSE)
        );
        private static final MethodHandle squareDistanceI8BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT8, Operation.BULK_SPARSE)
        );
        private static final MethodHandle dotProductF32BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.FLOAT32, Operation.BULK_SPARSE)
        );
        private static final MethodHandle squareDistanceF32BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.FLOAT32, Operation.BULK_SPARSE)
        );
        private static final MethodHandle dotProductDBF16QF32BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BFloat16QueryType.FLOAT32, Operation.BULK_SPARSE)
        );
        private static final MethodHandle squareDistanceDBF16QF32BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, BFloat16QueryType.FLOAT32, Operation.BULK_SPARSE)
        );
        private static final MethodHandle dotProductDBF16QBF16BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BFloat16QueryType.BFLOAT16, Operation.BULK_SPARSE)
        );
        private static final MethodHandle squareDistanceDBF16QBF16BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, BFloat16QueryType.BFLOAT16, Operation.BULK_SPARSE)
        );
        private static final MethodHandle dotProductD1Q4BulkSparse$mh = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D1Q4, Operation.BULK_SPARSE)
        );

        // --- INT7U: dot product and square distance ---

        @Override
        public int dotProductI7u(MemorySegment a, MemorySegment b, int length) {
            return dotProductI7uChecked(a, b, length);
        }

        @Override
        public void dotProductI7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulk(DataType.INT7U.bits(), a, b, length, count, scores);
            try {
                dotProductI7uBulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductI7uBulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBulkOffsets(DataType.INT7U.bits(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductI7uBulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductI7uBulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulkSparse(DataType.INT7U.bits(), addresses, b, length, count, scores);
            try {
                dotProductI7uBulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public int squareDistanceI7u(MemorySegment a, MemorySegment b, int length) {
            return squareDistanceI7uChecked(a, b, length);
        }

        @Override
        public void squareDistanceI7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulk(DataType.INT7U.bits(), a, b, length, count, scores);
            try {
                squareDistanceI7uBulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void squareDistanceI7uBulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBulkOffsets(DataType.INT7U.bits(), a, b, length, pitch, offsets, count, scores);
            try {
                squareDistanceI7uBulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void squareDistanceI7uBulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulkSparse(DataType.INT7U.bits(), addresses, b, length, count, scores);
            try {
                squareDistanceI7uBulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        // --- INT4: dot product only ---

        @Override
        public int dotProductI4(MemorySegment a, MemorySegment b, int length) {
            return dotProductI4Checked(a, b, length);
        }

        @Override
        public void dotProductI4Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulk(DataType.INT4.bits(), a, b, length, count, scores);
            try {
                dotProductI4Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductI4BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBulkOffsets(DataType.INT4.bits(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductI4BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductI4BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulkSparse(DataType.INT4.bits(), addresses, b, length, count, scores);
            try {
                dotProductI4BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        // --- INT8: cosine, dot product, square distance ---

        @Override
        public float cosineI8(MemorySegment a, MemorySegment b, int length) {
            return cosineI8Checked(a, b, length);
        }

        @Override
        public void cosineI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulk(DataType.INT8.bits(), a, b, length, count, scores);
            try {
                cosineI8Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void cosineI8BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBulkOffsets(DataType.INT8.bits(), a, b, length, pitch, offsets, count, scores);
            try {
                cosineI8BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void cosineI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulkSparse(DataType.INT8.bits(), addresses, b, length, count, scores);
            try {
                cosineI8BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public float dotProductI8(MemorySegment a, MemorySegment b, int length) {
            return dotProductI8Checked(a, b, length);
        }

        @Override
        public void dotProductI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulk(DataType.INT8.bits(), a, b, length, count, scores);
            try {
                dotProductI8Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductI8BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBulkOffsets(DataType.INT8.bits(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductI8BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulkSparse(DataType.INT8.bits(), addresses, b, length, count, scores);
            try {
                dotProductI8BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public float squareDistanceI8(MemorySegment a, MemorySegment b, int length) {
            return squareDistanceI8Checked(a, b, length);
        }

        @Override
        public void squareDistanceI8Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulk(DataType.INT8.bits(), a, b, length, count, scores);
            try {
                squareDistanceI8Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void squareDistanceI8BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBulkOffsets(DataType.INT8.bits(), a, b, length, pitch, offsets, count, scores);
            try {
                squareDistanceI8BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void squareDistanceI8BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulkSparse(DataType.INT8.bits(), addresses, b, length, count, scores);
            try {
                squareDistanceI8BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        // --- FLOAT32: dot product and square distance ---

        @Override
        public float dotProductF32(MemorySegment a, MemorySegment b, int length) {
            return dotProductF32Checked(a, b, length);
        }

        @Override
        public void dotProductF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulk(DataType.FLOAT32.bits(), a, b, length, count, scores);
            try {
                dotProductF32Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulkSparse(DataType.FLOAT32.bits(), addresses, b, length, count, scores);
            try {
                dotProductF32BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductF32BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBulkOffsets(DataType.FLOAT32.bits(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductF32BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public float squareDistanceF32(MemorySegment a, MemorySegment b, int length) {
            return squareDistanceF32Checked(a, b, length);
        }

        @Override
        public void squareDistanceF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulk(DataType.FLOAT32.bits(), a, b, length, count, scores);
            try {
                squareDistanceF32Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void squareDistanceF32BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBulkOffsets(DataType.FLOAT32.bits(), a, b, length, pitch, offsets, count, scores);
            try {
                squareDistanceF32BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void squareDistanceF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBulkSparse(DataType.FLOAT32.bits(), addresses, b, length, count, scores);
            try {
                squareDistanceF32BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        // --- BFloat16 ---

        @Override
        public float dotProductDBF16QF32(MemorySegment a, MemorySegment b, int length) {
            return dotProductDBF16QF32Checked(a, b, length);
        }

        @Override
        public void dotProductDBF16QF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBFloat16Bulk(BFloat16QueryType.FLOAT32.bytes(), a, b, length, count, scores);
            try {
                dotProductDBF16QF32Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductDBF16QF32BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBFloat16BulkSparse(BFloat16QueryType.FLOAT32.bytes(), addresses, b, length, count, scores);
            try {
                dotProductDBF16QF32BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductDBF16QF32BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBFloat16BulkOffsets(BFloat16QueryType.FLOAT32.bytes(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductDBF16QF32BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public float squareDistanceDBF16QF32(MemorySegment a, MemorySegment b, int length) {
            return squareDistanceDBF16QF32Checked(a, b, length);
        }

        @Override
        public void squareDistanceDBF16QF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBFloat16Bulk(BFloat16QueryType.FLOAT32.bytes(), a, b, length, count, scores);
            try {
                squareDistanceDBF16QF32Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void squareDistanceDBF16QF32BulkSparse(
            MemorySegment addresses,
            MemorySegment b,
            int length,
            int count,
            MemorySegment scores
        ) {
            checkBFloat16BulkSparse(BFloat16QueryType.FLOAT32.bytes(), addresses, b, length, count, scores);
            try {
                squareDistanceDBF16QF32BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void squareDistanceDBF16QF32BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBFloat16BulkOffsets(BFloat16QueryType.FLOAT32.bytes(), a, b, length, pitch, offsets, count, scores);
            try {
                squareDistanceDBF16QF32BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public float dotProductDBF16QBF16(MemorySegment a, MemorySegment b, int length) {
            return dotProductDBF16QBF16Checked(a, b, length);
        }

        @Override
        public void dotProductDBF16QBF16Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBFloat16Bulk(BFloat16QueryType.BFLOAT16.bytes(), a, b, length, count, scores);
            try {
                dotProductDBF16QBF16Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductDBF16QBF16BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBFloat16BulkSparse(BFloat16QueryType.BFLOAT16.bytes(), addresses, b, length, count, scores);
            try {
                dotProductDBF16QBF16BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductDBF16QBF16BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBFloat16BulkOffsets(BFloat16QueryType.BFLOAT16.bytes(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductDBF16QBF16BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public float squareDistanceDBF16QBF16(MemorySegment a, MemorySegment b, int length) {
            return squareDistanceDBF16QBF16Checked(a, b, length);
        }

        @Override
        public void squareDistanceDBF16QBF16Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBFloat16Bulk(BFloat16QueryType.BFLOAT16.bytes(), a, b, length, count, scores);
            try {
                squareDistanceDBF16QBF16Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void squareDistanceDBF16QBF16BulkSparse(
            MemorySegment addresses,
            MemorySegment b,
            int length,
            int count,
            MemorySegment scores
        ) {
            checkBFloat16BulkSparse(BFloat16QueryType.BFLOAT16.bytes(), addresses, b, length, count, scores);
            try {
                squareDistanceDBF16QBF16BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void squareDistanceDBF16QBF16BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBFloat16BulkOffsets(BFloat16QueryType.BFLOAT16.bytes(), a, b, length, pitch, offsets, count, scores);
            try {
                squareDistanceDBF16QBF16BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        // --- BBQ: dot product for all BBQ types ---

        @Override
        public long dotProductD1Q1(MemorySegment a, MemorySegment b, int length) {
            return dotProductD1Q1Checked(a, b, length);
        }

        @Override
        public void dotProductD1Q1Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBBQBulk(BBQType.D1Q1.queryBytesPerDocByte(), a, b, length, count, scores);
            try {
                dotProductD1Q1Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductD1Q1BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBBQBulkOffsets(BBQType.D1Q1.queryBytesPerDocByte(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductD1Q1BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public long dotProductD1Q4(MemorySegment a, MemorySegment b, int length) {
            return dotProductD1Q4Checked(a, b, length);
        }

        @Override
        public void dotProductD1Q4Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBBQBulk(BBQType.D1Q4.queryBytesPerDocByte(), a, b, length, count, scores);
            try {
                dotProductD1Q4Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductD1Q4BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBBQBulkOffsets(BBQType.D1Q4.queryBytesPerDocByte(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductD1Q4BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductD1Q4BulkSparse(MemorySegment addresses, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBBQBulkSparse(BBQType.D1Q4.queryBytesPerDocByte(), addresses, b, length, count, scores);
            try {
                dotProductD1Q4BulkSparse$mh.invokeExact(addresses, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public long dotProductD2Q2(MemorySegment a, MemorySegment b, int length) {
            return dotProductD2Q2Checked(a, b, length);
        }

        @Override
        public void dotProductD2Q2Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBBQBulk(BBQType.D2Q2.queryBytesPerDocByte(), a, b, length, count, scores);
            try {
                dotProductD2Q2Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductD2Q2BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBBQBulkOffsets(BBQType.D2Q2.queryBytesPerDocByte(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductD2Q2BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public long dotProductD2Q4(MemorySegment a, MemorySegment b, int length) {
            return dotProductD2Q4Checked(a, b, length);
        }

        @Override
        public void dotProductD2Q4Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBBQBulk(BBQType.D2Q4.queryBytesPerDocByte(), a, b, length, count, scores);
            try {
                dotProductD2Q4Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductD2Q4BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBBQBulkOffsets(BBQType.D2Q4.queryBytesPerDocByte(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductD2Q4BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public long dotProductD2Q4Packed(MemorySegment a, MemorySegment b, int length) {
            return dotProductD2Q4_PACKEDChecked(a, b, length);
        }

        @Override
        public void dotProductD2Q4PackedBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBBQBulk(BBQType.D2Q4_PACKED.queryBytesPerDocByte(), a, b, length, count, scores);
            try {
                dotProductD2Q4PackedBulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductD2Q4PackedBulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBBQBulkOffsets(BBQType.D2Q4_PACKED.queryBytesPerDocByte(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductD2Q4PackedBulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public long dotProductD4Q4(MemorySegment a, MemorySegment b, int length) {
            return dotProductD4Q4Checked(a, b, length);
        }

        @Override
        public void dotProductD4Q4Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment scores) {
            checkBBQBulk(BBQType.D4Q4.queryBytesPerDocByte(), a, b, length, count, scores);
            try {
                dotProductD4Q4Bulk$mh.invokeExact(a, b, length, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public void dotProductD4Q4BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment scores
        ) {
            checkBBQBulkOffsets(BBQType.D4Q4.queryBytesPerDocByte(), a, b, length, pitch, offsets, count, scores);
            try {
                dotProductD4Q4BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, scores);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        // --- Corrections (DiskBBQ) ---

        @Override
        public float applyCorrectionsEuclideanBulk(
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

        @Override
        public float applyCorrectionsMaxInnerProductBulk(
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

        @Override
        public float applyCorrectionsDotProductBulk(
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

        // --- Corrections (BBQ inline layout) ---

        @Override
        public float bbqApplyCorrectionsEuclideanBulk(
            MemorySegment data,
            int bulkSize,
            int vectorSizeInBytes,
            int pitchInBytes,
            int dimensions,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            float queryBitScale,
            float indexBitScale,
            float centroidDp,
            byte readComponentSumAsInt,
            MemorySegment scores
        ) {
            try {
                return (float) bbqApplyCorrectionsEuclideanBulk$mh.invokeExact(
                    data,
                    bulkSize,
                    vectorSizeInBytes,
                    pitchInBytes,
                    dimensions,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    queryBitScale,
                    indexBitScale,
                    centroidDp,
                    readComponentSumAsInt,
                    scores
                );
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public float bbqApplyCorrectionsMaxInnerProductBulk(
            MemorySegment data,
            int bulkSize,
            int vectorSizeInBytes,
            int pitchInBytes,
            int dimensions,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            float queryBitScale,
            float indexBitScale,
            float centroidDp,
            byte readComponentSumAsInt,
            MemorySegment scores
        ) {
            try {
                return (float) bbqApplyCorrectionsMaxInnerProductBulk$mh.invokeExact(
                    data,
                    bulkSize,
                    vectorSizeInBytes,
                    pitchInBytes,
                    dimensions,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    queryBitScale,
                    indexBitScale,
                    centroidDp,
                    readComponentSumAsInt,
                    scores
                );
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        @Override
        public float bbqApplyCorrectionsDotProductBulk(
            MemorySegment data,
            int bulkSize,
            int vectorSizeInBytes,
            int pitchInBytes,
            int dimensions,
            float queryLowerInterval,
            float queryUpperInterval,
            int queryComponentSum,
            float queryAdditionalCorrection,
            float queryBitScale,
            float indexBitScale,
            float centroidDp,
            byte readComponentSumAsInt,
            MemorySegment scores
        ) {
            try {
                return (float) bbqApplyCorrectionsDotProductBulk$mh.invokeExact(
                    data,
                    bulkSize,
                    vectorSizeInBytes,
                    pitchInBytes,
                    dimensions,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    queryBitScale,
                    indexBitScale,
                    centroidDp,
                    readComponentSumAsInt,
                    scores
                );
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }
    }
}
