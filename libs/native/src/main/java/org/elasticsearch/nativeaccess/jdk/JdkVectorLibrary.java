/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.lib.LoaderHelper;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.ref.Reference;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

public final class JdkVectorLibrary implements VectorLibrary {

    /**
     * A record holding multiple method handles, binding to the same function with different argument layouts.
     * <p>
     * Java code will initiate the call passing two MemorySegments.
     * Underneath, the C function is the same, accepting two pointer arguments.
     * The invoke code is free to choose which {@link MethodHandle} to use, and pass down the memory addresses as either MemorySegments,
     * longs, or a combination of the two.
     * <p>
     * The reason for passing down an address using {@link MemorySegment#address()} directly as a
     * {@link java.lang.foreign.ValueLayout#JAVA_LONG} is that the JVM performs additional checks on {@link MemorySegment}s when they are
     * shared (like we use in most cases), by injecting some additional code, including a CAS operation. This is generally OK, but in case
     * of high performance code the overhead can be significant; the cost of a CAS operation on x64 is ~15-30 cycles. This cost is well
     * amortized in case of bulk operations, but we measured up to a 30% slowdown on single scorers on x64.
     * <p>
     * However when {@link} some cases
     *
     * @param ll the {@link MethodHandle} to invoke the native function using longs for both addresses
     * @param la the {@link MethodHandle} to invoke the native function using long for the first address and {@link MemorySegment} for
     *           the second address
     * @param al the {@link MethodHandle} to invoke the native function using {@link MemorySegment} for the first address and long for
     *           the second address
     * @param aa the {@link MethodHandle} to invoke the native function using {@link MemorySegment}s for both addresses
     */
    private record SingleVectorMethodHandles(MethodHandle ll, MethodHandle la, MethodHandle al, MethodHandle aa) {
        private static FunctionDescriptor makeDescriptor(MemoryLayout resLayout, Stream<MemoryLayout> args, MemoryLayout... otherArgs) {
            return FunctionDescriptor.of(resLayout, Stream.concat(args, Arrays.stream(otherArgs)).toArray(MemoryLayout[]::new));
        }

        static SingleVectorMethodHandles create(String functionName, MemoryLayout result) {
            return new SingleVectorMethodHandles(
                downcallHandle(
                    functionName,
                    makeDescriptor(result, Stream.of(JAVA_LONG, JAVA_LONG), JAVA_INT),
                    LinkerHelperUtil.critical()
                ),
                downcallHandle(functionName, makeDescriptor(result, Stream.of(JAVA_LONG, ADDRESS), JAVA_INT), LinkerHelperUtil.critical()),
                downcallHandle(functionName, makeDescriptor(result, Stream.of(ADDRESS, JAVA_LONG), JAVA_INT), LinkerHelperUtil.critical()),
                downcallHandle(functionName, makeDescriptor(result, Stream.of(ADDRESS, ADDRESS), JAVA_INT), LinkerHelperUtil.critical())
            );
        }

        private static Error invocationError(Throwable t, MemorySegment segment1, MemorySegment segment2) {
            String msg = "Invocation failed: "
                + "first segment=["
                + segment1
                + ", scope="
                + segment1.scope()
                + ", isAlive="
                + segment1.scope().isAlive()
                + "], "
                + "second segment=["
                + segment2
                + ", scope="
                + segment2.scope()
                + ", isAlive="
                + segment2.scope().isAlive()
                + "]";
            return new AssertionError(msg, t);
        }

        int invokeInt(MemorySegment a, MemorySegment query, int length) {
            try {
                var aAddress = a.address();
                var queryAddress = query.address();

                if (aAddress != 0) {
                    assert a.isNative();
                    if (queryAddress != 0) {
                        assert query.isNative();
                        try {
                            return (int) ll.invokeExact(aAddress, queryAddress, length);
                        } finally {
                            assert a.scope().isAlive();
                            assert query.scope().isAlive();
                            // protects the segment from being potentially being GC'ed during out downcall
                            Reference.reachabilityFence(a);
                            Reference.reachabilityFence(query);
                        }
                    }
                    try {
                        return (int) la.invokeExact(aAddress, query, length);
                    } finally {
                        assert a.scope().isAlive();
                        Reference.reachabilityFence(a);
                    }
                }
                if (queryAddress != 0) {
                    assert query.isNative();
                    try {
                        return (int) al.invokeExact(a, queryAddress, length);
                    } finally {
                        assert query.scope().isAlive();
                        Reference.reachabilityFence(query);
                    }
                }
                return (int) aa.invokeExact(a, query, length);
            } catch (Throwable t) {
                throw invocationError(t, a, query);
            }
        }

        long invokeLong(MemorySegment a, MemorySegment query, int length) {
            try {
                var aAddress = a.address();
                var queryAddress = query.address();

                if (aAddress != 0) {
                    assert a.isNative();
                    if (queryAddress != 0) {
                        assert query.isNative();
                        try {
                            return (long) ll.invokeExact(aAddress, queryAddress, length);
                        } finally {
                            assert a.scope().isAlive();
                            assert query.scope().isAlive();
                            // protects the segment from being potentially being GC'ed during out downcall
                            Reference.reachabilityFence(a);
                            Reference.reachabilityFence(query);
                        }
                    }
                    try {
                        return (long) la.invokeExact(aAddress, query, length);
                    } finally {
                        assert a.scope().isAlive();
                        Reference.reachabilityFence(a);
                    }
                }
                if (queryAddress != 0) {
                    assert query.isNative();
                    try {
                        return (long) al.invokeExact(a, queryAddress, length);
                    } finally {
                        assert query.scope().isAlive();
                        Reference.reachabilityFence(query);
                    }
                }
                return (long) aa.invokeExact(a, query, length);
            } catch (Throwable t) {
                throw invocationError(t, a, query);
            }
        }

        float invokeFloat(MemorySegment a, MemorySegment query, int length) {
            try {
                var aAddress = a.address();
                var queryAddress = query.address();

                if (aAddress != 0) {
                    assert a.isNative();
                    if (queryAddress != 0) {
                        assert query.isNative();
                        try {
                            return (float) ll.invokeExact(aAddress, queryAddress, length);
                        } finally {
                            assert a.scope().isAlive();
                            assert query.scope().isAlive();
                            // protects the segment from being potentially being GC'ed during out downcall
                            Reference.reachabilityFence(a);
                            Reference.reachabilityFence(query);
                        }
                    }
                    try {
                        return (float) la.invokeExact(aAddress, query, length);
                    } finally {
                        assert a.scope().isAlive();
                        Reference.reachabilityFence(a);
                    }
                }
                if (queryAddress != 0) {
                    assert query.isNative();
                    try {
                        return (float) al.invokeExact(a, queryAddress, length);
                    } finally {
                        assert query.scope().isAlive();
                        Reference.reachabilityFence(query);
                    }
                }
                return (float) aa.invokeExact(a, query, length);
            } catch (Throwable t) {
                throw invocationError(t, a, query);
            }
        }
    }

    static final Logger logger = LogManager.getLogger(JdkVectorLibrary.class);

    static final SingleVectorMethodHandles dot7u$mh;
    static final MethodHandle dot7uBulk$mh;
    static final MethodHandle dot7uBulkWithOffsets$mh;

    static final MethodHandle sqr7u$mh;
    static final MethodHandle sqr7uBulk$mh;
    static final MethodHandle sqr7uBulkWithOffsets$mh;

    static final SingleVectorMethodHandles dotf32$mh;
    static final MethodHandle dotf32Bulk$mh;
    static final MethodHandle dotf32BulkWithOffsets$mh;

    static final MethodHandle sqrf32$mh;
    static final MethodHandle sqrf32Bulk$mh;
    static final MethodHandle sqrf32BulkWithOffsets$mh;

    public static final JdkVectorSimilarityFunctions INSTANCE;

    static {
        LoaderHelper.loadLibrary("vec");
        final MethodHandle vecCaps$mh = downcallHandle("vec_caps", FunctionDescriptor.of(JAVA_INT));

        try {
            int caps = (int) vecCaps$mh.invokeExact();
            logger.info("vec_caps=" + caps);
            if (caps > 0) {
                String suffix = caps == 2 ? "_2" : "";
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

                dot7u$mh = SingleVectorMethodHandles.create("vec_dot7u" + suffix, JAVA_INT);
                dot7uBulk$mh = downcallHandle("vec_dot7u_bulk" + suffix, bulk, LinkerHelperUtil.critical());
                dot7uBulkWithOffsets$mh = downcallHandle("vec_dot7u_bulk_offsets" + suffix, bulkOffsets, LinkerHelperUtil.critical());

                sqr7u$mh = downcallHandle("vec_sqr7u" + suffix, intSingle, LinkerHelperUtil.critical());
                sqr7uBulk$mh = downcallHandle("vec_sqr7u_bulk" + suffix, bulk, LinkerHelperUtil.critical());
                sqr7uBulkWithOffsets$mh = downcallHandle("vec_sqr7u_bulk_offsets" + suffix, bulkOffsets, LinkerHelperUtil.critical());

                dotf32$mh = SingleVectorMethodHandles.create("vec_dotf32" + suffix, JAVA_FLOAT);
                dotf32Bulk$mh = downcallHandle("vec_dotf32_bulk" + suffix, bulk, LinkerHelperUtil.critical());
                dotf32BulkWithOffsets$mh = downcallHandle("vec_dotf32_bulk_offsets" + suffix, bulkOffsets, LinkerHelperUtil.critical());

                sqrf32$mh = downcallHandle("vec_sqrf32" + suffix, floatSingle, LinkerHelperUtil.critical());
                sqrf32Bulk$mh = downcallHandle("vec_sqrf32_bulk" + suffix, bulk, LinkerHelperUtil.critical());
                sqrf32BulkWithOffsets$mh = downcallHandle("vec_sqrf32_bulk_offsets" + suffix, bulkOffsets, LinkerHelperUtil.critical());

                INSTANCE = new JdkVectorSimilarityFunctions();
            } else {
                if (caps < 0) {
                    logger.warn("""
                        Your CPU supports vector capabilities, but they are disabled at OS level. For optimal performance, \
                        enable them in your OS/Hypervisor/VM/container""");
                }
                dot7u$mh = null;
                dot7uBulk$mh = null;
                dot7uBulkWithOffsets$mh = null;
                sqr7u$mh = null;
                sqr7uBulk$mh = null;
                sqr7uBulkWithOffsets$mh = null;
                dotf32$mh = null;
                dotf32Bulk$mh = null;
                dotf32BulkWithOffsets$mh = null;
                sqrf32$mh = null;
                sqrf32Bulk$mh = null;
                sqrf32BulkWithOffsets$mh = null;
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
         * Computes the dot product of given unsigned int7 byte vectors.
         *
         * <p> Unsigned int7 byte vectors have values in the range of 0 to 127 (inclusive).
         *
         * @param a      address of the first vector
         * @param b      address of the second vector
         * @param length the vector dimensions
         */
        static int dotProduct7u(MemorySegment a, MemorySegment b, int length) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, length, (int) a.byteSize());
            return dot7u$mh.invokeInt(a, b, length);
        }

        static void dotProduct7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            Objects.checkFromIndexSize(0, length * count, (int) a.byteSize());
            Objects.checkFromIndexSize(0, length, (int) b.byteSize());
            Objects.checkFromIndexSize(0, count * Float.BYTES, (int) result.byteSize());
            dot7uBulk(a, b, length, count, result);
        }

        static void dotProduct7uBulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            dot7uBulkWithOffsets(a, b, length, pitch, offsets, count, result);
        }

        /**
         * Computes the square distance of given unsigned int7 byte vectors.
         *
         * <p> Unsigned int7 byte vectors have values in the range of 0 to 127 (inclusive).
         *
         * @param a      address of the first vector
         * @param b      address of the second vector
         * @param length the vector dimensions
         */
        static int squareDistance7u(MemorySegment a, MemorySegment b, int length) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, length, (int) a.byteSize());
            return sqr7u(a, b, length);
        }

        static void squareDistance7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            Objects.checkFromIndexSize(0, length * count, (int) a.byteSize());
            Objects.checkFromIndexSize(0, length, (int) b.byteSize());
            Objects.checkFromIndexSize(0, count * Float.BYTES, (int) result.byteSize());
            sqr7uBulk(a, b, length, count, result);
        }

        static void squareDistance7uBulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            sqr7uBulkWithOffsets(a, b, length, pitch, offsets, count, result);
        }

        /**
         * Computes the dot product of given float32 vectors.
         *
         * @param a      address of the first vector
         * @param b      address of the second vector
         * @param elementCount the vector dimensions, number of float32 elements in the segment
         */
        static float dotProductF32(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, elementCount, (int) a.byteSize() / Float.BYTES);
            return dotf32$mh.invokeFloat(a, b, elementCount);
        }

        static void dotProductF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            Objects.checkFromIndexSize(0, length * count * Float.BYTES, (int) a.byteSize());
            Objects.checkFromIndexSize(0, length * Float.BYTES, (int) b.byteSize());
            Objects.checkFromIndexSize(0, count * Float.BYTES, (int) result.byteSize());
            dotf32Bulk(a, b, length, count, result);
        }

        static void dotProductF32BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            // pitch is in bytes, but needs to be 4-byte-aligned
            if ((pitch % 4) != 0) throw new IllegalArgumentException("Pitch needs to be a multiple of 4");
            dotf32BulkWithOffsets(a, b, length, pitch, offsets, count, result);
        }

        /**
         * Computes the square distance of given float32 vectors.
         *
         * @param a      address of the first vector
         * @param b      address of the second vector
         * @param elementCount the vector dimensions, number of float32 elements in the segment
         */
        static float squareDistanceF32(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, elementCount, (int) a.byteSize() / Float.BYTES);
            return sqrf32(a, b, elementCount);
        }

        static void squareDistanceF32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            Objects.checkFromIndexSize(0, length * count * Float.BYTES, (int) a.byteSize());
            Objects.checkFromIndexSize(0, length * Float.BYTES, (int) b.byteSize());
            Objects.checkFromIndexSize(0, count * Float.BYTES, (int) result.byteSize());
            sqrf32Bulk(a, b, length, count, result);
        }

        static void squareDistanceF32BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            // pitch is in bytes, but needs to be 4-byte-aligned
            if ((pitch % 4) != 0) throw new IllegalArgumentException("Pitch needs to be a multiple of 4");
            sqrf32BulkWithOffsets(a, b, length, pitch, offsets, count, result);
        }

        private static void checkByteSize(MemorySegment a, MemorySegment b) {
            if (a.byteSize() != b.byteSize()) {
                throw new IllegalArgumentException("dimensions differ: " + a.byteSize() + "!=" + b.byteSize());
            }
        }

        private static void dot7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            try {
                dot7uBulk$mh.invokeExact(a, b, length, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static void dot7uBulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            try {
                dot7uBulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static int sqr7u(MemorySegment a, MemorySegment b, int length) {
            try {
                return (int) sqr7u$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static void sqr7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            try {
                sqr7uBulk$mh.invokeExact(a, b, length, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static void sqr7uBulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            try {
                sqr7uBulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static void dotf32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            try {
                dotf32Bulk$mh.invokeExact(a, b, length, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static void dotf32BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            try {
                dotf32BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static float sqrf32(MemorySegment a, MemorySegment b, int length) {
            try {
                return (float) sqrf32$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static void sqrf32Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            try {
                sqrf32Bulk$mh.invokeExact(a, b, length, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static void sqrf32BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            try {
                sqrf32BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        static final MethodHandle DOT_HANDLE_7U;
        static final MethodHandle DOT_HANDLE_7U_BULK;
        static final MethodHandle DOT_HANDLE_7U_BULK_WITH_OFFSETS;
        static final MethodHandle SQR_HANDLE_7U;
        static final MethodHandle SQR_HANDLE_7U_BULK;
        static final MethodHandle SQR_HANDLE_7U_BULK_WITH_OFFSETS;
        static final MethodHandle DOT_HANDLE_FLOAT32;
        static final MethodHandle DOT_HANDLE_FLOAT32_BULK;
        static final MethodHandle DOT_HANDLE_FLOAT32_BULK_WITH_OFFSETS;
        static final MethodHandle SQR_HANDLE_FLOAT32;
        static final MethodHandle SQR_HANDLE_FLOAT32_BULK;
        static final MethodHandle SQR_HANDLE_FLOAT32_BULK_WITH_OFFSETS;

        static {
            try {
                var lookup = MethodHandles.lookup();

                MethodType singleInt7Scorer = MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class, int.class);
                MethodType singleFloatScorer = MethodType.methodType(float.class, MemorySegment.class, MemorySegment.class, int.class);
                MethodType bulkScorer = MethodType.methodType(
                    void.class,
                    MemorySegment.class,
                    MemorySegment.class,
                    int.class,
                    int.class,
                    MemorySegment.class
                );
                MethodType bulkOffsetScorer = MethodType.methodType(
                    void.class,
                    MemorySegment.class,
                    MemorySegment.class,
                    int.class,
                    int.class,
                    MemorySegment.class,
                    int.class,
                    MemorySegment.class
                );

                DOT_HANDLE_7U = lookup.findStatic(JdkVectorSimilarityFunctions.class, "dotProduct7u", singleInt7Scorer);
                DOT_HANDLE_7U_BULK = lookup.findStatic(JdkVectorSimilarityFunctions.class, "dotProduct7uBulk", bulkScorer);
                DOT_HANDLE_7U_BULK_WITH_OFFSETS = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "dotProduct7uBulkWithOffsets",
                    bulkOffsetScorer
                );

                SQR_HANDLE_7U = lookup.findStatic(JdkVectorSimilarityFunctions.class, "squareDistance7u", singleInt7Scorer);
                SQR_HANDLE_7U_BULK = lookup.findStatic(JdkVectorSimilarityFunctions.class, "squareDistance7uBulk", bulkScorer);
                SQR_HANDLE_7U_BULK_WITH_OFFSETS = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "squareDistance7uBulkWithOffsets",
                    bulkOffsetScorer
                );

                DOT_HANDLE_FLOAT32 = lookup.findStatic(JdkVectorSimilarityFunctions.class, "dotProductF32", singleFloatScorer);
                DOT_HANDLE_FLOAT32_BULK = lookup.findStatic(JdkVectorSimilarityFunctions.class, "dotProductF32Bulk", bulkScorer);
                DOT_HANDLE_FLOAT32_BULK_WITH_OFFSETS = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "dotProductF32BulkWithOffsets",
                    bulkOffsetScorer
                );

                SQR_HANDLE_FLOAT32 = lookup.findStatic(JdkVectorSimilarityFunctions.class, "squareDistanceF32", singleFloatScorer);
                SQR_HANDLE_FLOAT32_BULK = lookup.findStatic(JdkVectorSimilarityFunctions.class, "squareDistanceF32Bulk", bulkScorer);
                SQR_HANDLE_FLOAT32_BULK_WITH_OFFSETS = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "squareDistanceF32BulkWithOffsets",
                    bulkOffsetScorer
                );
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public MethodHandle dotProductHandle7u() {
            return DOT_HANDLE_7U;
        }

        @Override
        public MethodHandle dotProductHandle7uBulk() {
            return DOT_HANDLE_7U_BULK;
        }

        @Override
        public MethodHandle dotProductHandle7uBulkWithOffsets() {
            return DOT_HANDLE_7U_BULK_WITH_OFFSETS;
        }

        @Override
        public MethodHandle squareDistanceHandle7u() {
            return SQR_HANDLE_7U;
        }

        @Override
        public MethodHandle squareDistanceHandle7uBulk() {
            return SQR_HANDLE_7U_BULK;
        }

        @Override
        public MethodHandle squareDistanceHandle7uBulkWithOffsets() {
            return SQR_HANDLE_7U_BULK_WITH_OFFSETS;
        }

        @Override
        public MethodHandle dotProductHandleFloat32() {
            return DOT_HANDLE_FLOAT32;
        }

        @Override
        public MethodHandle dotProductHandleFloat32Bulk() {
            return DOT_HANDLE_FLOAT32_BULK;
        }

        @Override
        public MethodHandle dotProductHandleFloat32BulkWithOffsets() {
            return DOT_HANDLE_FLOAT32_BULK_WITH_OFFSETS;
        }

        @Override
        public MethodHandle squareDistanceHandleFloat32() {
            return SQR_HANDLE_FLOAT32;
        }

        @Override
        public MethodHandle squareDistanceHandleFloat32Bulk() {
            return SQR_HANDLE_FLOAT32_BULK;
        }

        @Override
        public MethodHandle squareDistanceHandleFloat32BulkWithOffsets() {
            return SQR_HANDLE_FLOAT32_BULK_WITH_OFFSETS;
        }

    }
}
