/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;

/**
 * Utility methods to act on MemorySegment apis which have changed in subsequent JDK releases.
 */
class MemorySegmentUtil {

    static String getString(MemorySegment segment, long offset) {
        return segment.getUtf8String(offset);
    }

    static void setString(MemorySegment segment, long offset, String value) {
        segment.setUtf8String(offset, value);
    }

    static MemorySegment allocateString(Arena arena, String s) {
        return arena.allocateUtf8String(s);
    }

    /**
     * Return a {@link VarHandle} to access an element within the given memory segment.
     *
     * Note: This is no-op in Java 21, see the Java 22 implementation.
     *
     * @param layout The layout of a struct to access
     * @param element The element within the struct to access
     * @return A {@link VarHandle} that accesses the element with a fixed offset of 0
     */
    static VarHandle varHandleWithoutOffset(MemoryLayout layout, MemoryLayout.PathElement element) {
        return layout.varHandle(element);
    }

    private MemorySegmentUtil() {}
}
