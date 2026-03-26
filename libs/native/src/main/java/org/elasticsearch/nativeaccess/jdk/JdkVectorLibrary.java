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

                FunctionDescriptor bulkSparse = FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS);

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
                            case BULK_SPARSE -> "_bulk_sparse";
                        };

                        for (DataType type : DataType.values()) {
                            // Only byte vectors have cosine
                            // as floats are normalized to unit length to use dot_product instead
                            if (f == Function.COSINE && type != DataType.INT8) continue;
                            // Only DOT_PRODUCT is needed for int4 — other functions are computed by
                            // applying correction terms on top of the raw dot product result.
                            if (f != Function.DOT_PRODUCT && type == DataType.INT4) continue;
                            // BULK_SPARSE only for INT7U and INT8 — no native sparse functions exist for FLOAT32 or INT4
                            if (op == Operation.BULK_SPARSE && (type == DataType.FLOAT32 || type == DataType.INT4)) continue;

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

                            MethodHandle handle = bindFunction("vec_" + funcName + typeName + opName, caps, descriptor);
                            handles.put(new OperationSignature<>(f, type, op), handle);
                        }

                        for (BBQType type : BBQType.values()) {
                            // not implemented yet...
                            if (f == Function.COSINE || f == Function.SQUARE_DISTANCE) continue;
                            // BULK_SPARSE not yet implemented for BBQ
                            if (op == Operation.BULK_SPARSE) continue;

                            String typeName = switch (type) {
                                case D1Q4 -> "d1q4";
                                case D2Q4 -> "d2q4";
                                case D4Q4 -> "d4q4";
                            };

                            FunctionDescriptor descriptor = switch (op) {
                                case SINGLE -> longSingle;
                                case BULK, BULK_SPARSE -> bulk;
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

        static boolean checkBulk(int elementBits, MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            Objects.checkFromIndexSize(0L, (long) length * count * elementBits / 8, a.byteSize());
            Objects.checkFromIndexSize(0L, length, b.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
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
            Objects.checkFromIndexSize(0L, (long) datasetVectorLengthInBytes * count, dataset.byteSize());
            // 1 bit data -> x4 bits query, 2 bit data -> x2 bits query
            Objects.checkFromIndexSize(0L, (long) datasetVectorLengthInBytes * (queryBits / dataBits), query.byteSize());
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
            assert elementBits % 8 == 0 : "requires byte-aligned element types";
            Objects.checkFromIndexSize(0L, (long) count * Long.BYTES, addresses.byteSize());
            Objects.checkFromIndexSize(0L, (long) length * elementBits / 8, b.byteSize());
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
            if (pitch < rowBytes) throw new IllegalArgumentException("Pitch needs to be at least " + length);
            Objects.checkFromIndexSize(0L, (long) pitch * count, a.byteSize());
            Objects.checkFromIndexSize(0L, rowBytes, b.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Integer.BYTES, offsets.byteSize());
            Objects.checkFromIndexSize(0L, (long) count * Float.BYTES, result.byteSize());
            assert validateBulkOffsets(a, offsets, count, length, pitch, result, rowBytes);
            return true;
        }

        static boolean checkBBQBulkOffsets(
            int dataBits,
            MemorySegment a,
            MemorySegment b,
            int datasetVectorLengthInBytes,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            final int queryBits = 4;
            if (pitch < datasetVectorLengthInBytes) throw new IllegalArgumentException(
                "Pitch needs to be at least " + datasetVectorLengthInBytes
            );
            Objects.checkFromIndexSize(0L, (long) datasetVectorLengthInBytes * count, a.byteSize());
            // 1 bit data -> x4 bits query, 2 bit data -> x2 bits query
            Objects.checkFromIndexSize(0L, (long) datasetVectorLengthInBytes * (queryBits / dataBits), b.byteSize());
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
            long vectorBytes = (long) length * elementBits / 8;
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

        static int dotProductI7u(MemorySegment a, MemorySegment b, int length) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceInt(dotI7uHandle, a, b, length);
        }

        private static final MethodHandle squareI7uHandle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT7U, Operation.SINGLE)
        );

        static int squareDistanceI7u(MemorySegment a, MemorySegment b, int length) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceInt(squareI7uHandle, a, b, length);
        }

        private static final MethodHandle dotI4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT4, Operation.SINGLE)
        );

        static int dotProductI4(MemorySegment a, MemorySegment b, int elementCount) {
            Objects.checkFromIndexSize(0L, 2L * elementCount, a.byteSize());
            Objects.checkFromIndexSize(0L, elementCount, b.byteSize());
            return callSingleDistanceInt(dotI4Handle, a, b, elementCount);
        }

        private static final MethodHandle cosI8Handle = HANDLES.get(
            new OperationSignature<>(Function.COSINE, DataType.INT8, Operation.SINGLE)
        );

        static float cosineI8(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize());
            return callSingleDistanceFloat(cosI8Handle, a, b, elementCount);
        }

        private static final MethodHandle dotI8Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.INT8, Operation.SINGLE)
        );

        static float dotProductI8(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize());
            return callSingleDistanceFloat(dotI8Handle, a, b, elementCount);
        }

        private static final MethodHandle squareI8Handle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.INT8, Operation.SINGLE)
        );

        static float squareDistanceI8(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize());
            return callSingleDistanceFloat(squareI8Handle, a, b, elementCount);
        }

        private static final MethodHandle dotF32Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, DataType.FLOAT32, Operation.SINGLE)
        );

        static float dotProductF32(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize() / Float.BYTES);
            return callSingleDistanceFloat(dotF32Handle, a, b, elementCount);
        }

        private static final MethodHandle squareF32Handle = HANDLES.get(
            new OperationSignature<>(Function.SQUARE_DISTANCE, DataType.FLOAT32, Operation.SINGLE)
        );

        static float squareDistanceF32(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0L, elementCount, a.byteSize() / Float.BYTES);
            return callSingleDistanceFloat(squareF32Handle, a, b, elementCount);
        }

        private static final MethodHandle dotD1Q4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D1Q4, Operation.SINGLE)
        );

        static long dotProductD1Q4(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0L, (long) length * 4, query.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceLong(dotD1Q4Handle, a, query, length);
        }

        private static final MethodHandle dotD2Q4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D2Q4, Operation.SINGLE)
        );

        static long dotProductD2Q4(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0L, (long) length * 2, query.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
            return callSingleDistanceLong(dotD2Q4Handle, a, query, length);
        }

        private static final MethodHandle dotD4Q4Handle = HANDLES.get(
            new OperationSignature<>(Function.DOT_PRODUCT, BBQType.D4Q4, Operation.SINGLE)
        );

        static long dotProductD4Q4(MemorySegment a, MemorySegment query, int length) {
            Objects.checkFromIndexSize(0L, length, query.byteSize());
            Objects.checkFromIndexSize(0L, length, a.byteSize());
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
                                        case INT4:
                                            type = MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class, int.class);
                                            checkMethod += "I4";
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
                                        MethodHandles.insertArguments(checkMethod, 0, dt.bits()),
                                        op.getValue(),
                                        MethodHandles.empty(op.getValue().type())
                                    );
                                }
                                default -> throw new IllegalArgumentException("Unknown handle type " + op.getKey().dataType());
                            };

                            handlesWithChecks.put(op.getKey(), handleWithChecks);
                        }
                        case BULK_SPARSE -> {
                            MethodHandle handleWithChecks = switch (op.getKey().dataType()) {
                                case DataType dt -> {
                                    MethodHandle checkMethod = lookup.findStatic(
                                        JdkVectorSimilarityFunctions.class,
                                        "checkBulkSparse",
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
                                        MethodHandles.insertArguments(checkMethod, 0, dt.bits()),
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
                                case BBQType bbq -> {
                                    MethodHandle checkMethod = lookup.findStatic(
                                        JdkVectorSimilarityFunctions.class,
                                        "checkBBQBulkOffsets",
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
                                        MethodHandles.insertArguments(checkMethod, 0, bbq.dataBits()),
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
                                        MethodHandles.insertArguments(checkMethod, 0, dt.bits()),
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
