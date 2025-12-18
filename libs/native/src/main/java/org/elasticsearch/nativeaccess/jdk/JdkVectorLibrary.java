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
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Objects;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

public final class JdkVectorLibrary implements VectorLibrary {

    static final Logger logger = LogManager.getLogger(JdkVectorLibrary.class);

    static final MethodHandle dot7u$mh;
    static final MethodHandle dot7uBulk$mh;
    static final MethodHandle dot7uBulkWithOffsets$mh;

    static final MethodHandle doti4b1$mh;
    static final MethodHandle doti4b1Bulk$mh;
    static final MethodHandle doti4b1BulkWithOffsets$mh;

    static final MethodHandle sqr7u$mh;
    static final MethodHandle cosf32$mh;
    static final MethodHandle dotf32$mh;
    static final MethodHandle sqrf32$mh;

    public static final JdkVectorSimilarityFunctions INSTANCE;

    static {
        LoaderHelper.loadLibrary("vec");
        final MethodHandle vecCaps$mh = downcallHandle("vec_caps", FunctionDescriptor.of(JAVA_INT));

        try {
            int caps = (int) vecCaps$mh.invokeExact();
            logger.info("vec_caps=" + caps);
            if (caps > 0) {
                if (caps == 2) {
                    dot7u$mh = downcallHandle(
                        "vec_dot7u_2",
                        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    dot7uBulk$mh = downcallHandle(
                        "vec_dot7u_bulk_2",
                        FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS),
                        LinkerHelperUtil.critical()
                    );
                    dot7uBulkWithOffsets$mh = downcallHandle(
                        "vec_dot7u_bulk_offsets_2",
                        FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS),
                        LinkerHelperUtil.critical()
                    );
                    doti4b1$mh = downcallHandle(
                        "vec_dot_bit_int4_2",
                        FunctionDescriptor.of(JAVA_LONG, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    doti4b1Bulk$mh = downcallHandle(
                        "vec_dot_bit_int4_bulk_2",
                        FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS),
                        LinkerHelperUtil.critical()
                    );
                    doti4b1BulkWithOffsets$mh = downcallHandle(
                        "vec_dot_bit_int4_bulk_offsets_2",
                        FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS),
                        LinkerHelperUtil.critical()
                    );
                    sqr7u$mh = downcallHandle(
                        "vec_sqr7u_2",
                        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    cosf32$mh = downcallHandle(
                        "vec_cosf32_2",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    dotf32$mh = downcallHandle(
                        "vec_dotf32_2",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    sqrf32$mh = downcallHandle(
                        "vec_sqrf32_2",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                } else {
                    dot7u$mh = downcallHandle(
                        "vec_dot7u",
                        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    dot7uBulk$mh = downcallHandle(
                        "vec_dot7u_bulk",
                        FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS),
                        LinkerHelperUtil.critical()
                    );
                    dot7uBulkWithOffsets$mh = downcallHandle(
                        "vec_dot7u_bulk_offsets",
                        FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS),
                        LinkerHelperUtil.critical()
                    );
                    doti4b1$mh = downcallHandle(
                        "vec_dot_bit_int4_2",
                        FunctionDescriptor.of(JAVA_LONG, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    doti4b1Bulk$mh = downcallHandle(
                        "vec_dot_bit_int4_bulk_2",
                        FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS),
                        LinkerHelperUtil.critical()
                    );
                    doti4b1BulkWithOffsets$mh = downcallHandle(
                        "vec_dot_bit_int4_bulk_offsets_2",
                        FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS),
                        LinkerHelperUtil.critical()
                    );
                    sqr7u$mh = downcallHandle(
                        "vec_sqr7u",
                        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    cosf32$mh = downcallHandle(
                        "vec_cosf32",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    dotf32$mh = downcallHandle(
                        "vec_dotf32",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    sqrf32$mh = downcallHandle(
                        "vec_sqrf32",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                }
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
                doti4b1$mh = null;
                doti4b1Bulk$mh = null;
                doti4b1BulkWithOffsets$mh = null;
                sqr7u$mh = null;
                cosf32$mh = null;
                dotf32$mh = null;
                sqrf32$mh = null;
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
            return dot7u(a, b, length);
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
         * Computes the dot product of a given int4 vector with a give bit vector (1 bit per element).
         *
         * @param a      address of the int4 vector
         * @param b      address of the bit vector
         * @param length the vector dimensions
         */
        static long dotProductI4B1(MemorySegment a, MemorySegment b, int length) {
            if (a.byteSize() != 4L * length) {
                throw new IllegalArgumentException("dimensions differ: " + a.byteSize() + "!=" + 4L * length);
            }
            Objects.checkFromIndexSize(0, length, (int) a.byteSize());
            return doti4b1(a, b, length);
        }

        static void dotProductI4B1Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            Objects.checkFromIndexSize(0, length * count, (int) a.byteSize());
            Objects.checkFromIndexSize(0, length, (int) b.byteSize());
            Objects.checkFromIndexSize(0, count * Float.BYTES, (int) result.byteSize());
            doti4b1Bulk(a, b, length, count, result);
        }

        static void dotProductI4B1BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            doti4b1BulkWithOffsets(a, b, length, pitch, offsets, count, result);
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

        /**
         * Computes the cosine of given float32 vectors.
         *
         * @param a      address of the first vector
         * @param b      address of the second vector
         * @param elementCount the vector dimensions, number of float32 elements in the segment
         */
        static float cosineF32(MemorySegment a, MemorySegment b, int elementCount) {
            checkByteSize(a, b);
            Objects.checkFromIndexSize(0, elementCount, (int) a.byteSize() / Float.BYTES);
            return cosf32(a, b, elementCount);
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
            return dotf32(a, b, elementCount);
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

        private static void checkByteSize(MemorySegment a, MemorySegment b) {
            if (a.byteSize() != b.byteSize()) {
                throw new IllegalArgumentException("dimensions differ: " + a.byteSize() + "!=" + b.byteSize());
            }
        }

        private static int dot7u(MemorySegment a, MemorySegment b, int length) {
            try {
                return (int) JdkVectorLibrary.dot7u$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static void dot7uBulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            try {
                JdkVectorLibrary.dot7uBulk$mh.invokeExact(a, b, length, count, result);
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
                JdkVectorLibrary.dot7uBulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static long doti4b1(MemorySegment a, MemorySegment b, int length) {
            try {
                return (long) JdkVectorLibrary.doti4b1$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static void doti4b1Bulk(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {
            try {
                JdkVectorLibrary.doti4b1Bulk$mh.invokeExact(a, b, length, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static void doti4b1BulkWithOffsets(
            MemorySegment a,
            MemorySegment b,
            int length,
            int pitch,
            MemorySegment offsets,
            int count,
            MemorySegment result
        ) {
            try {
                JdkVectorLibrary.doti4b1BulkWithOffsets$mh.invokeExact(a, b, length, pitch, offsets, count, result);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static int sqr7u(MemorySegment a, MemorySegment b, int length) {
            try {
                return (int) JdkVectorLibrary.sqr7u$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static float cosf32(MemorySegment a, MemorySegment b, int length) {
            try {
                return (float) JdkVectorLibrary.cosf32$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static float dotf32(MemorySegment a, MemorySegment b, int length) {
            try {
                return (float) JdkVectorLibrary.dotf32$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static float sqrf32(MemorySegment a, MemorySegment b, int length) {
            try {
                return (float) JdkVectorLibrary.sqrf32$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        static final MethodHandle DOT_HANDLE_7U;
        static final MethodHandle DOT_HANDLE_7U_BULK;
        static final MethodHandle DOT_HANDLE_7U_BULK_WITH_OFFSETS;

        static final MethodHandle DOT_HANDLE_I4B1;
        static final MethodHandle DOT_HANDLE_I4B1_BULK;
        static final MethodHandle DOT_HANDLE_I4B1_BULK_WITH_OFFSETS;

        static final MethodHandle SQR_HANDLE_7U;
        static final MethodHandle COS_HANDLE_FLOAT32;
        static final MethodHandle DOT_HANDLE_FLOAT32;
        static final MethodHandle SQR_HANDLE_FLOAT32;

        static {
            try {
                var lookup = MethodHandles.lookup();
                var mt = MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class, int.class);
                DOT_HANDLE_7U = lookup.findStatic(JdkVectorSimilarityFunctions.class, "dotProduct7u", mt);
                SQR_HANDLE_7U = lookup.findStatic(JdkVectorSimilarityFunctions.class, "squareDistance7u", mt);

                mt = MethodType.methodType(void.class, MemorySegment.class, MemorySegment.class, int.class, int.class, MemorySegment.class);
                DOT_HANDLE_7U_BULK = lookup.findStatic(JdkVectorSimilarityFunctions.class, "dotProduct7uBulk", mt);

                DOT_HANDLE_7U_BULK_WITH_OFFSETS = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "dotProduct7uBulkWithOffsets",
                    MethodType.methodType(
                        void.class,
                        MemorySegment.class,
                        MemorySegment.class,
                        int.class,
                        int.class,
                        MemorySegment.class,
                        int.class,
                        MemorySegment.class
                    )
                );

                DOT_HANDLE_I4B1 = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "dotProductI4B1",
                    MethodType.methodType(long.class, MemorySegment.class, MemorySegment.class, int.class)
                );
                DOT_HANDLE_I4B1_BULK = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "dotProductI4B1Bulk",
                    MethodType.methodType(void.class, MemorySegment.class, MemorySegment.class, int.class, int.class, MemorySegment.class)
                );
                DOT_HANDLE_I4B1_BULK_WITH_OFFSETS = lookup.findStatic(
                    JdkVectorSimilarityFunctions.class,
                    "dotProductI4B1BulkWithOffsets",
                    MethodType.methodType(
                        void.class,
                        MemorySegment.class,
                        MemorySegment.class,
                        int.class,
                        int.class,
                        MemorySegment.class,
                        int.class,
                        MemorySegment.class
                    )
                );

                mt = MethodType.methodType(float.class, MemorySegment.class, MemorySegment.class, int.class);
                COS_HANDLE_FLOAT32 = lookup.findStatic(JdkVectorSimilarityFunctions.class, "cosineF32", mt);
                DOT_HANDLE_FLOAT32 = lookup.findStatic(JdkVectorSimilarityFunctions.class, "dotProductF32", mt);
                SQR_HANDLE_FLOAT32 = lookup.findStatic(JdkVectorSimilarityFunctions.class, "squareDistanceF32", mt);
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException(e);
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
        public MethodHandle dotProductHandleI4B1() {
            return DOT_HANDLE_I4B1;
        }

        @Override
        public MethodHandle dotProductHandleI4B1Bulk() {
            return DOT_HANDLE_I4B1_BULK;
        }

        @Override
        public MethodHandle dotProductHandleI4B1BulkWithOffsets() {
            return DOT_HANDLE_I4B1_BULK_WITH_OFFSETS;
        }

        @Override
        public MethodHandle squareDistanceHandle7u() {
            return SQR_HANDLE_7U;
        }

        @Override
        public MethodHandle cosineHandleFloat32() {
            return COS_HANDLE_FLOAT32;
        }

        @Override
        public MethodHandle dotProductHandleFloat32() {
            return DOT_HANDLE_FLOAT32;
        }

        @Override
        public MethodHandle squareDistanceHandleFloat32() {
            return SQR_HANDLE_FLOAT32;
        }
    }
}
