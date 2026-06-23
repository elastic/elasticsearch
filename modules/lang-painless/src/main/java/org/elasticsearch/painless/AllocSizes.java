/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

/**
 * Sizing constants and helpers for Painless allocation tracking. Sizes are derived purely from the allocation's structure
 * (element type and count, captured-value count, boxed primitive) as known at compile time -- no reflection. All constants
 * assume HotSpot 64-bit with conservative reference sizing (8 bytes), which is safe under both {@code +UseCompressedOops}
 * (the ES default) and {@code -UseCompressedOops} (large-heap configs); sizing may trip the limit slightly earlier than
 * strictly necessary on compressed-oops JVMs, but never under-counts these structurally-sized allocations.
 *
 * <p>Sizing the object produced by {@code new T()} needs the class's field layout, which is the whitelist's domain: it is
 * determined at whitelist-load time and carried on the constructor metadata (see the {@code @allocates} annotation work),
 * not computed reflectively here.
 */
public final class AllocSizes {

    /** JVM object header size in bytes (HotSpot 64-bit). */
    public static final int OBJECT_HEADER = 12;

    /** JVM array header size in bytes (object header + 4-byte length field). */
    public static final int ARRAY_HEADER = 16;

    /**
     * Conservative reference size in bytes. Over-counts by 2x on compressed-oops JVMs (the ES default, where refs are
     * 4 bytes) but is safe under all supported JVM configs.
     */
    public static final int REFERENCE_SIZE = 8;

    private AllocSizes() {}

    /** Rounds {@code bytes} up to the nearest 8-byte alignment boundary, saturating rather than overflowing near {@link Long#MAX_VALUE}. */
    public static long pad8(long bytes) {
        if (bytes > Long.MAX_VALUE - 7L) {
            return Long.MAX_VALUE;
        }
        return (bytes + 7L) & ~7L;
    }

    /**
     * Signed multiply that saturates to {@link Long#MAX_VALUE}/{@link Long#MIN_VALUE} on overflow instead of wrapping. Used
     * to fold multi-dimensional array extents into a single element count: a wrapped product could under-count an enormous
     * allocation and let it slip past the limit, so overflow must clamp high (the saturated charge then trips the limit)
     * rather than silently wrap to a small value.
     */
    public static long mulSat(long a, long b) {
        long low = a * b;
        // The full 128-bit product fits in a signed long iff its high half is the sign extension of the low half.
        if (Math.multiplyHigh(a, b) == (low >> 63)) {
            return low;
        }
        // Overflow: clamp toward the sign the true product would have had (opposite signs -> negative).
        return ((a ^ b) < 0) ? Long.MIN_VALUE : Long.MAX_VALUE;
    }

    /** Signed add that saturates to {@link Long#MAX_VALUE}/{@link Long#MIN_VALUE} on overflow instead of wrapping. */
    private static long addSat(long a, long b) {
        long r = a + b;
        // Overflow occurred iff both operands share a sign that differs from the result's.
        if (((a ^ r) & (b ^ r)) < 0) {
            return r < 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
        return r;
    }

    /** Returns the in-memory footprint of one field/element of the given type; references count as {@link #REFERENCE_SIZE}. */
    public static int fieldSize(Class<?> type) {
        if (type == long.class || type == double.class) {
            return 8;
        }
        if (type == int.class || type == float.class) {
            return 4;
        }
        if (type == short.class || type == char.class) {
            return 2;
        }
        if (type == byte.class || type == boolean.class) {
            return 1;
        }
        return REFERENCE_SIZE;
    }

    /**
     * Returns the heap size of the boxed wrapper for {@code type}, which may be given as either the primitive (as the cast
     * emitter sees it) or the wrapper class. {@code long}/{@code double} (and {@link Long}/{@link Double}) carry an 8-byte
     * value (24 bytes total); all other numeric wrappers carry at most 4 bytes (16 bytes total).
     */
    public static long boxSize(Class<?> type) {
        if (type == long.class || type == double.class || type == Long.class || type == Double.class) {
            return pad8(OBJECT_HEADER + 8L); // 24
        }
        return pad8(OBJECT_HEADER + 4L); // 16
    }

    /** Returns the heap size of a one-dimensional array with {@code length} elements of {@code componentType}. */
    public static long arraySize(Class<?> componentType, long length) {
        return arrayBytes(length, fieldSize(componentType));
    }

    /**
     * Returns the heap size of an array of {@code length} elements each {@code fieldSize} bytes wide, using saturating
     * arithmetic so an overflowing extent yields {@link Long#MAX_VALUE} (which trips any limit) rather than a wrapped,
     * under-counted charge. Invoked directly from generated bytecode for runtime-sized arrays, where {@code length} is the
     * (already saturating) product of the dimensions; see {@code DefaultIRTreeToASMBytesPhase#visitNewArray}.
     */
    public static long arrayBytes(long length, int fieldSize) {
        return pad8(addSat(ARRAY_HEADER, mulSat(fieldSize, length)));
    }

    /** Returns the heap size of a lambda/reference capture object holding {@code captureCount} captured references. */
    public static long captureSize(int captureCount) {
        return pad8(ARRAY_HEADER + (long) REFERENCE_SIZE * captureCount);
    }
}
