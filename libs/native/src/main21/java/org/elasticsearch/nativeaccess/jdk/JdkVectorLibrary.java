/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.lib.LoaderHelper;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

public final class JdkVectorLibrary implements VectorLibrary {

    static final MethodHandle dot7u$mh;
    static final MethodHandle sqr7u$mh;

    static final VectorSimilarityFunctions INSTANCE;

    static {
        LoaderHelper.loadLibrary("vec");
        final MethodHandle vecCaps$mh = downcallHandle("vec_caps", FunctionDescriptor.of(JAVA_INT));

        try {
            int caps = (int) vecCaps$mh.invokeExact();
            if (caps != 0) {
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
                }
                INSTANCE = new JdkVectorSimilarityFunctions();
            } else {
                dot7u$mh = null;
                sqr7u$mh = null;
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
            assert length >= 0;
            if (a.byteSize() != b.byteSize()) {
                throw new IllegalArgumentException("dimensions differ: " + a.byteSize() + "!=" + b.byteSize());
            }
            if (length > a.byteSize()) {
                throw new IllegalArgumentException("length: " + length + ", greater than vector dimensions: " + a.byteSize());
            }
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
            assert length >= 0;
            if (a.byteSize() != b.byteSize()) {
                throw new IllegalArgumentException("dimensions differ: " + a.byteSize() + "!=" + b.byteSize());
            }
            if (length > a.byteSize()) {
                throw new IllegalArgumentException("length: " + length + ", greater than vector dimensions: " + a.byteSize());
            }
            return sqr7u(a, b, length);
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

        static final MethodHandle DOT_HANDLE_7U;
        static final MethodHandle SQR_HANDLE_7U;

        static {
            try {
                var lookup = MethodHandles.lookup();
                var mt = MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class, int.class);
                DOT_HANDLE_7U = lookup.findStatic(JdkVectorSimilarityFunctions.class, "dotProduct7u", mt);
                SQR_HANDLE_7U = lookup.findStatic(JdkVectorSimilarityFunctions.class, "squareDistance7u", mt);
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
    }
}
