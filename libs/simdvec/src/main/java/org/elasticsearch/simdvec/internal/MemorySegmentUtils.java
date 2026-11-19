/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.elasticsearch.lucene.store.ThreadConfinedArena;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

final class MemorySegmentUtils {

    private MemorySegmentUtils() {}

    static MemorySegment nativeCopyOf(float[] array) {
        return nativeCopyOf(array, null);
    }

    static MemorySegment nativeCopyOf(float[] array, Arena arena) {
        MemorySegment segment = allocate(arena, (long) array.length * Float.BYTES, ValueLayout.JAVA_LONG.byteAlignment());
        MemorySegment.copy(array, 0, segment, ValueLayout.JAVA_FLOAT, 0, array.length);
        return segment;
    }

    static MemorySegment nativeCopyOf(byte[] array) {
        return nativeCopyOf(array, null);
    }

    static MemorySegment nativeCopyOf(byte[] array, Arena arena) {
        MemorySegment segment = allocate(arena, array.length, ValueLayout.JAVA_LONG.byteAlignment());
        MemorySegment.copy(array, 0, segment, ValueLayout.JAVA_BYTE, 0, array.length);
        return segment;
    }

    private static MemorySegment allocate(Arena arena, long byteSize, long byteAlignment) {
        return arena != null ? arena.allocate(byteSize, byteAlignment) : ThreadConfinedArena.allocate(byteSize, byteAlignment);
    }
}
