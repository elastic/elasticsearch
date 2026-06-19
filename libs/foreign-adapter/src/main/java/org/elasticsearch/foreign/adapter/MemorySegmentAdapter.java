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
public class MemorySegmentAdapter {

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
     * Returns a {@link VarHandle} to access an element within the given memory layout.
     * On JDK 21 this is a no-op pass-through; on JDK 22+ the returned VarHandle has an
     * extra offset coordinate that must be fixed up with {@code insertCoordinates}.
     *
     * @param layout  the layout of a struct to access
     * @param element the element within the struct to access
     * @return a {@link VarHandle} that accesses the element with a fixed offset of 0
     */
    public static VarHandle varHandleWithoutOffset(MemoryLayout layout, MemoryLayout.PathElement element) {
        return layout.varHandle(element);
    }

    private MemorySegmentAdapter() {}
}
