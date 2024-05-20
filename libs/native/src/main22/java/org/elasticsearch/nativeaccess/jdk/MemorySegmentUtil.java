/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

class MemorySegmentUtil {

    static String getString(MemorySegment segment, long offset) {
        return segment.getString(offset);
    }

    static MemorySegment allocateString(Arena arena, String s) {
        return arena.allocateFrom(s);
    }

    // MemorySegment.varHandle changed between 21 and 22. The resulting varHandle now requires an additional
    // long offset parameter. We omit the offset at runtime, instead binding to the VarHandle in JDK 22.
    static VarHandle varHandleDropOffset(VarHandle varHandle) {
        return MethodHandles.insertCoordinates(varHandle, 1, 0L);
    }

    private MemorySegmentUtil() {}
}
