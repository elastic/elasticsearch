/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.foreign.MemoryLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Helpers for building {@link VarHandle}s over a {@link MemoryLayout} that omit the leading base-offset
 * coordinate.
 *
 * <p>{@link MemoryLayout#varHandle(MemoryLayout.PathElement...)} returns a {@code VarHandle} whose first
 * coordinate is a {@code long} base offset into the accessed {@link java.lang.foreign.MemorySegment}. Struct
 * field access always uses offset {@code 0}, so these helpers bind that coordinate to {@code 0} up front,
 * leaving a {@code VarHandle} shaped as {@code (MemorySegment)} for scalars and {@code (MemorySegment, long index)}
 * for sequence elements. This keeps call sites and generated accessors free of a constant {@code 0L} argument.
 */
public final class MemoryLayoutVarHandles {

    private MemoryLayoutVarHandles() {}

    /** Returns a {@code VarHandle} for the scalar field selected by {@code element}, with the base offset bound to 0. */
    public static VarHandle varHandleWithoutOffset(MemoryLayout layout, MemoryLayout.PathElement element) {
        return MethodHandles.insertCoordinates(layout.varHandle(element), 1, 0L);
    }

    /**
     * Returns a {@code VarHandle} for indexed sequence-element access, with the base offset bound to 0. The
     * returned handle takes {@code (segment, index)} for reads and {@code (segment, index, value)} for writes.
     */
    public static VarHandle varHandleSequenceWithoutOffset(
        MemoryLayout layout,
        MemoryLayout.PathElement group,
        MemoryLayout.PathElement seq
    ) {
        return MethodHandles.insertCoordinates(layout.varHandle(group, seq), 1, 0L);
    }
}
