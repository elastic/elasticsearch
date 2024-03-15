/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk.vec;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

final class NativeVectorDistance {

    static {
        loadLibrary();
    }

    @SuppressWarnings("removal")
    private static void loadLibrary() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            System.loadLibrary("vec");
            return null;
        });
    }

    private static final MethodHandle stride$mh = downcallHandle("stride", FunctionDescriptor.of(JAVA_INT));
    private static final MethodHandle dot8s$mh = downcallHandle("dot8s", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));

    // Stride of the native implementation - consumes this number of bytes per loop invocation.
    // There must be at least this number of bytes/elements available when going native
    static final int STRIDE = 32;

    static {
        assert STRIDE > 0 && (STRIDE & (STRIDE - 1)) == 0 : "Not a power of two";
        assert stride() == STRIDE;
    }

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
        int i = 0;
        int res = 0;
        if (length >= STRIDE) {
            i += length & ~(STRIDE - 1);
            res = dot8s(a, b, i);
        }

        // tail
        for (; i < length; i++) {
            res += a.get(JAVA_BYTE, i) * b.get(JAVA_BYTE, i);
        }
        assert i == length;
        return res;
    }

    static float squareDistance(MemorySegment a, MemorySegment b, int length) {
        return 0f; // TODO
    }

    private static int stride() {
        try {
            return (int) stride$mh.invokeExact();
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
}
