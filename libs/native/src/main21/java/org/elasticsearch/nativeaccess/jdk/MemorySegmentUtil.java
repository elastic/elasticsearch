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

/**
 * Utility methods to act on MemorySegment apis which have changed in subsequent JDK releases.
 */
class MemorySegmentUtil {

    static String getString(MemorySegment segment, long offset) {
        return segment.getUtf8String(offset);
    }

    static MemorySegment allocateString(Arena arena, String s) {
        return arena.allocateUtf8String(s);
    }

    // MemorySegment.varHandle changed between 21 and 22. The resulting varHandle now requires an additional
    // long offset parameter. We pass the offset in at runtime, and drop this offset for 21.
    static VarHandle varHandleDropOffset(VarHandle varHandle) {
        return MethodHandles.dropCoordinates(varHandle, 1, Long.class);
    }

    private MemorySegmentUtil() {}
}
