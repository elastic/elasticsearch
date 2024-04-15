/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

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

    static {
        System.loadLibrary("vec");
    }

    public JdkVectorLibrary() {}

    static final MethodHandle dot8s$mh = downcallHandle("dot8s", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
    static final MethodHandle sqr8s$mh = downcallHandle("sqr8s", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));

    /**
     * Computes the dot product of given byte vectors.
     * @param a address of the first vector
     * @param b address of the second vector
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
        return dot8s(a, b, length);
    }

    /**
     * Computes the square distance of given byte vectors.
     * @param a address of the first vector
     * @param b address of the second vector
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
        return sqr8s(a, b, length);
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
            DOT_HANDLE = lookup.findStatic(JdkVectorLibrary.class, "dotProduct", mt);
            SQR_HANDLE = lookup.findStatic(JdkVectorLibrary.class, "squareDistance", mt);
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
