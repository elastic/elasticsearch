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
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.functionAddressOrNull;

public final class JdkVectorLibrary implements VectorLibrary {

    static final Logger logger = LogManager.getLogger(JdkVectorLibrary.class);

    static final MethodHandle dot7u$mh;
    static final MethodHandle dot7uBulk$mh;
    static final MethodHandle dot7uBulkWithOffsets$mh;
    static final MethodHandle sqr7u$mh;
    static final MethodHandle sqr7uBulk$mh;
    static final MethodHandle sqr7uBulkWithOffsets$mh;
    static final MethodHandle dotf32$mh;
    static final MethodHandle dotf32Bulk$mh;
    static final MethodHandle dotf32BulkWithOffsets$mh;
    static final MethodHandle sqrf32$mh;
    static final MethodHandle sqrf32Bulk$mh;
    static final MethodHandle sqrf32BulkWithOffsets$mh;

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
        final MethodHandle vecCaps$mh = downcallHandle("vec_caps", FunctionDescriptor.of(JAVA_INT));

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

                dot7u$mh = bindFunction("vec_dot7u", caps, intSingle);
                dot7uBulk$mh = bindFunction("vec_dot7u_bulk", caps, bulk);
                dot7uBulkWithOffsets$mh = bindFunction("vec_dot7u_bulk_offsets", caps, bulkOffsets);
                sqr7u$mh = bindFunction("vec_sqr7u", caps, intSingle);
                sqr7uBulk$mh = bindFunction("vec_sqr7u_bulk", caps, bulk);
                sqr7uBulkWithOffsets$mh = bindFunction("vec_sqr7u_bulk_offsets", caps, bulkOffsets);
                dotf32$mh = bindFunction("vec_dotf32", caps, floatSingle);
                dotf32Bulk$mh = bindFunction("vec_dotf32_bulk", caps, bulk);
                dotf32BulkWithOffsets$mh = bindFunction("vec_dotf32_bulk_offsets", caps, bulkOffsets);
                sqrf32$mh = bindFunction("vec_sqrf32", caps, floatSingle);
                sqrf32Bulk$mh = bindFunction("vec_sqrf32_bulk", caps, bulk);
                sqrf32BulkWithOffsets$mh = bindFunction("vec_sqrf32_bulk_offsets", caps, bulkOffsets);
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
            return dotf32(a, b, elementCount);
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

        private static int dot7u(MemorySegment a, MemorySegment b, int length) {
            try {
                return (int) dot7u$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
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

        private static float dotf32(MemorySegment a, MemorySegment b, int length) {
            try {
                return (float) dotf32$mh.invokeExact(a, b, length);
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
