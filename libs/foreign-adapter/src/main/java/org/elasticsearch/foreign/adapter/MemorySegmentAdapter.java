/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.adapter;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Adapts MemorySegment APIs that changed between JDK 21 and 22+.
 */
public final class MemorySegmentAdapter {

    public static String getString(MemorySegment segment, long offset) {
        return segment.getString(offset);
    }

    public static void setString(MemorySegment segment, long offset, String value) {
        segment.setString(offset, value);
    }

    public static MemorySegment allocateString(Arena arena, String s) {
        return arena.allocateFrom(s);
    }

    // MemoryLayout.varHandle changed between Java 21 and 22 to require a new offset
    // parameter for the returned VarHandle. This function exists to remove the need for that offset.
    public static VarHandle varHandleWithoutOffset(MemoryLayout layout, MemoryLayout.PathElement element) {
        return MethodHandles.insertCoordinates(layout.varHandle(element), 1, 0L);
    }

    /**
     * Return a {@link VarHandle} for indexed sequence element access within the given memory layout.
     * The Java 22 variant inserts a fixed offset coordinate at position 1 so callers pass
     * {@code (segment, 0L, (long) index)} for reads and {@code (segment, 0L, (long) index, value)}
     * for writes — matching the Java 21 two-path-element VarHandle shape.
     */
    public static VarHandle varHandleSequenceWithoutOffset(
        MemoryLayout layout,
        MemoryLayout.PathElement group,
        MemoryLayout.PathElement seq
    ) {
        return MethodHandles.insertCoordinates(layout.varHandle(group, seq), 1, 0L);
    }

    private MemorySegmentAdapter() {}
}
