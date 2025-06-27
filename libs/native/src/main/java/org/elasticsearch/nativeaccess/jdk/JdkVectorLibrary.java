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

public final class JdkVectorLibrary implements VectorLibrary {

    static final Logger logger = LogManager.getLogger(JdkVectorLibrary.class);

    static final MethodHandle dot7u$mh;
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
                        "dot7u_2",
                        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    sqr7u$mh = downcallHandle(
                        "sqr7u_2",
                        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    cosf32$mh = downcallHandle(
                        "cosf32_2",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    dotf32$mh = downcallHandle(
                        "dotf32_2",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    sqrf32$mh = downcallHandle(
                        "sqrf32_2",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                } else {
                    dot7u$mh = downcallHandle(
                        "dot7u",
                        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    sqr7u$mh = downcallHandle(
                        "sqr7u",
                        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    cosf32$mh = downcallHandle(
                        "cosf32",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    dotf32$mh = downcallHandle(
                        "dotf32",
                        FunctionDescriptor.of(JAVA_FLOAT, ADDRESS, ADDRESS, JAVA_INT),
                        LinkerHelperUtil.critical()
                    );
                    sqrf32$mh = downcallHandle(
                        "sqrf32",
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
