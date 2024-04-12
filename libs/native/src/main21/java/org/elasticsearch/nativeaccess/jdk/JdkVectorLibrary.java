/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

public final class JdkVectorLibrary implements VectorLibrary {

    static final VectorSimilarityFunctions INSTANCE;

    static {
        System.loadLibrary("vec");
        final MethodHandle vecCaps$mh = downcallHandle("vec_caps", FunctionDescriptor.of(JAVA_INT));

        try {
            int caps = (int) vecCaps$mh.invokeExact();
            if (caps != 0) {
                INSTANCE = new JdkVectorSimilarityFunctions();
            } else {
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

        static final MethodHandle dot8stride$mh = downcallHandle("dot8s_stride", FunctionDescriptor.of(JAVA_INT));
        static final MethodHandle sqr8stride$mh = downcallHandle("sqr8s_stride", FunctionDescriptor.of(JAVA_INT));

        static final MethodHandle dot8s$mh = downcallHandle("dot8s", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
        static final MethodHandle sqr8s$mh = downcallHandle("sqr8s", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));

        // Stride of the native implementation - consumes this number of bytes per loop invocation.
        // There must be at least this number of bytes/elements available when going native
        static final int DOT_STRIDE = 32;
        static final int SQR_STRIDE = 16;

        static {
            assert DOT_STRIDE > 0 && (DOT_STRIDE & (DOT_STRIDE - 1)) == 0 : "Not a power of two";
            assert dot8Stride() == DOT_STRIDE : dot8Stride() + " != " + DOT_STRIDE;
            assert SQR_STRIDE > 0 && (SQR_STRIDE & (SQR_STRIDE - 1)) == 0 : "Not a power of two";
            assert sqr8Stride() == SQR_STRIDE : sqr8Stride() + " != " + SQR_STRIDE;
        }

        /**
         * Computes the dot product of given byte vectors.
         *
         * @param a      address of the first vector
         * @param b      address of the second vector
         * @param length the vector dimensions
         */
        static int dotProduct(MemorySegment a, MemorySegment b, int length) {
            assert length >= 0;
            if (a.byteSize() != b.byteSize()) {
                throw new IllegalArgumentException("dimensions differ: " + a.byteSize() + "!=" + b.byteSize());
            }
            if (length > a.byteSize()) {
                throw new IllegalArgumentException("length: " + length + ", greater than vector dimensions: " + a.byteSize());
            }
            int i = 0;
            int res = 0;
            if (length >= DOT_STRIDE) {
                i += length & ~(DOT_STRIDE - 1);
                res = dot8s(a, b, i);
            }

            // tail
            for (; i < length; i++) {
                res += a.get(JAVA_BYTE, i) * b.get(JAVA_BYTE, i);
            }
            assert i == length;
            return res;
        }

        /**
         * Computes the square distance of given byte vectors.
         *
         * @param a      address of the first vector
         * @param b      address of the second vector
         * @param length the vector dimensions
         */
        static int squareDistance(MemorySegment a, MemorySegment b, int length) {
            assert length >= 0;
            if (a.byteSize() != b.byteSize()) {
                throw new IllegalArgumentException("dimensions differ: " + a.byteSize() + "!=" + b.byteSize());
            }
            if (length > a.byteSize()) {
                throw new IllegalArgumentException("length: " + length + ", greater than vector dimensions: " + a.byteSize());
            }
            int i = 0;
            int res = 0;
            if (length >= SQR_STRIDE) {
                i += length & ~(SQR_STRIDE - 1);
                res = sqr8s(a, b, i);
            }

            // tail
            for (; i < length; i++) {
                int dist = a.get(JAVA_BYTE, i) - b.get(JAVA_BYTE, i);
                res += dist * dist;
            }
            assert i == length;
            return res;
        }

        private static int dot8Stride() {
            try {
                return (int) dot8stride$mh.invokeExact();
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static int sqr8Stride() {
            try {
                return (int) sqr8stride$mh.invokeExact();
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static int dot8s(MemorySegment a, MemorySegment b, int length) {
            try {
                return (int) dot8s$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private static int sqr8s(MemorySegment a, MemorySegment b, int length) {
            try {
                return (int) sqr8s$mh.invokeExact(a, b, length);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        static final MethodHandle DOT_HANDLE;
        static final MethodHandle SQR_HANDLE;

        static {
            try {
                var lookup = MethodHandles.lookup();
                var mt = MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class, int.class);
                DOT_HANDLE = lookup.findStatic(JdkVectorSimilarityFunctions.class, "dotProduct", mt);
                SQR_HANDLE = lookup.findStatic(JdkVectorSimilarityFunctions.class, "squareDistance", mt);
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public MethodHandle dotProductHandle() {
            return DOT_HANDLE;
        }

        @Override
        public MethodHandle squareDistanceHandle() {
            return SQR_HANDLE;
        }
    }
}
