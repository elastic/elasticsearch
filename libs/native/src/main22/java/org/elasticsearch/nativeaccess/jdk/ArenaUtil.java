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
import java.nio.charset.Charset;

public class ArenaUtil {

    /**
     * Allocate an array of the given memory layout.
     */
    static MemorySegment allocate(Arena arena, MemoryLayout layout, int count) {
        return arena.allocate(layout, count);
    }

    /**
     * Allocate and copy the given string into native memory.
     */
    static MemorySegment allocateFrom(Arena arena, String str, Charset charset) {
        return arena.allocateFrom(str, charset);
    }

    private ArenaUtil() {}
}
