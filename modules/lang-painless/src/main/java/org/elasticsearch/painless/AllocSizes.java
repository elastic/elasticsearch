/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Compile-time sizing constants and helpers for Painless allocation tracking. All constants assume HotSpot 64-bit with
 * conservative reference sizing (8 bytes), which is safe under both {@code +UseCompressedOops} (the ES default) and
 * {@code -UseCompressedOops} (large-heap configs). Sizing may trip the limit slightly earlier than strictly necessary on
 * compressed-oops JVMs, but never under-counts a real allocation. Sizes are computed once at compile time and cached.
 */
public final class AllocSizes {

    private static final Logger logger = LogManager.getLogger(AllocSizes.class);

    /** JVM object header size in bytes (HotSpot 64-bit). */
    public static final int OBJECT_HEADER = 12;

    /** JVM array header size in bytes (object header + 4-byte length field). */
    public static final int ARRAY_HEADER = 16;

    /**
     * Conservative reference size in bytes. Over-counts by 2x on compressed-oops JVMs (the ES default, where refs are
     * 4 bytes) but is safe under all supported JVM configs.
     */
    public static final int REFERENCE_SIZE = 8;

    /** Conservative size used when reflective field-walking is blocked by the module system. */
    static final long FALLBACK_OBJECT_SIZE = 64L;

    private static final ConcurrentHashMap<Class<?>, Long> SIZE_CACHE = new ConcurrentHashMap<>();

    private AllocSizes() {}

    /** Rounds {@code bytes} up to the nearest 8-byte alignment boundary. */
    public static long pad8(long bytes) {
        return (bytes + 7L) & ~7L;
    }

    /**
     * Returns the heap size of a single instance of {@code type} via a reflective field-walk, cached after the first
     * computation. Falls back to {@link #FALLBACK_OBJECT_SIZE} (logging a one-time {@code WARN} per class) when reflective
     * access is denied by the module system.
     */
    public static long sizeOf(Class<?> type) {
        return SIZE_CACHE.computeIfAbsent(type, AllocSizes::computeSizeOf);
    }

    @SuppressForbidden(reason = "getDeclaredFields() is required to size private fields; only field metadata is read, never accessed")
    private static long computeSizeOf(Class<?> type) {
        long sum = OBJECT_HEADER;
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            try {
                for (Field field : current.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers()) == false) {
                        sum += fieldSize(field.getType());
                    }
                }
            } catch (RuntimeException e) {
                // InaccessibleObjectException (JPMS) or SecurityException: use a conservative fallback rather than failing
                // compilation. The over-count means the limit may trip slightly early but never silently misses a real OOM.
                logger.warn("could not reflectively size [{}] for Painless allocation tracking; using conservative fallback", type);
                return pad8(FALLBACK_OBJECT_SIZE);
            }
        }
        return pad8(sum);
    }

    /** Returns the in-memory footprint of one field of the given type; references count as {@link #REFERENCE_SIZE}. */
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
        return pad8(ARRAY_HEADER + (long) fieldSize(componentType) * length);
    }

    /** Returns the heap size of a lambda/reference capture object holding {@code captureCount} captured slots. */
    public static long captureSize(int captureCount) {
        return pad8(ARRAY_HEADER + (long) REFERENCE_SIZE * captureCount);
    }
}
