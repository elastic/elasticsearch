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
import java.lang.invoke.VarHandle;

/**
 * Adapts MemorySegment APIs that changed between JDK 21 and 22+.
 */
public final class MemorySegmentAdapter {

    public static String getString(MemorySegment segment, long offset) {
        return segment.getUtf8String(offset);
    }

    public static void setString(MemorySegment segment, long offset, String value) {
        segment.setUtf8String(offset, value);
    }

    public static MemorySegment allocateString(Arena arena, String s) {
        return arena.allocateUtf8String(s);
    }

    /**
     * Return a {@link VarHandle} to access an element within the given memory layout.
     *
     * Returns the VarHandle directly; in Java 21, a single-element path VarHandle does not need an
     * offset coordinate inserted (unlike the Java 22 variant, which inserts a fixed {@code 0L} at
     * coordinate position 1).
     *
     * @param layout The layout of a struct to access
     * @param element The element within the struct to access
     * @return A {@link VarHandle} that accesses the element with a fixed offset of 0
     */
    public static VarHandle varHandleWithoutOffset(MemoryLayout layout, MemoryLayout.PathElement element) {
        return layout.varHandle(element);
    }

    /**
     * Return a {@link VarHandle} to access a sequence element within the given memory layout,
     * using a two-element path: {@code groupElement(name)} then {@code sequenceElement()}.
     *
     * Returns the VarHandle directly; in Java 21, group+sequence path VarHandles do not need an
     * offset coordinate inserted (unlike the Java 22 variant, which inserts a fixed {@code 0L} at
     * coordinate position 1).
     *
     * @param layout The layout of a struct to access
     * @param group The group element path element (e.g. {@code groupElement("fieldName")})
     * @param seq The sequence element path element (i.e. {@code sequenceElement()})
     * @return A {@link VarHandle} that accesses indexed sequence elements with a fixed offset of 0
     */
    public static VarHandle varHandleSequenceWithoutOffset(
        MemoryLayout layout,
        MemoryLayout.PathElement group,
        MemoryLayout.PathElement seq
    ) {
        return layout.varHandle(group, seq);
    }

    private MemorySegmentAdapter() {}
}
