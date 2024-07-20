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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

class MemorySegmentUtil {

    static String getString(MemorySegment segment, long offset) {
        return segment.getString(offset);
    }

    static void setString(MemorySegment segment, long offset, String value) {
        segment.setString(offset, value);
    }

    static MemorySegment allocateString(Arena arena, String s) {
        return arena.allocateFrom(s);
    }

    // MemoryLayout.varHandle changed between Java 21 and 22 to require a new offset
    // parameter for the returned VarHandle. This function exists to remove the need for that offset.
    static VarHandle varHandleWithoutOffset(MemoryLayout layout, MemoryLayout.PathElement element) {
        return MethodHandles.insertCoordinates(layout.varHandle(element), 1, 0L);
    }

    private MemorySegmentUtil() {}
}
