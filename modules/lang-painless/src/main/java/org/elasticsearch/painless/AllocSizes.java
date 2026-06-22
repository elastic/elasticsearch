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

    /** Rounds {@code bytes} up to the nearest 8-byte alignment boundary. */
    public static long pad8(long bytes) {
        return (bytes + 7L) & ~7L;
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
        return pad8(ARRAY_HEADER + fieldSize(componentType) * length);
    }

    /** Returns the heap size of a lambda/reference capture object holding {@code captureCount} captured references. */
    public static long captureSize(int captureCount) {
        return pad8(ARRAY_HEADER + (long) REFERENCE_SIZE * captureCount);
    }
}
