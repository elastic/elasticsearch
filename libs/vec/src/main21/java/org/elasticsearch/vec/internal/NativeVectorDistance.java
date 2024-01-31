/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec.internal;

import org.elasticsearch.vec.internal.gen.vec_h;

import java.lang.foreign.MemorySegment;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class NativeVectorDistance {

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
            res = vec_h.dot8s(a, b, i);
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

    // Stride of the native implementation - consumes this number of bytes per loop invocation.
    // There must be at least this number of bytes/elements available when going native
    static final int STRIDE = 32;

    static {
        assert STRIDE > 0 && (STRIDE & (STRIDE - 1)) == 0 : "Not a power of two";
        assert vec_h.stride() == STRIDE;
    }
}
