/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

/**
 * Utility methods to act on Arena apis which have changed in subsequent JDK releases.
 */
class ArenaUtil {

    /**
     * Allocate an array of the given memory layout.
     */
    static MemorySegment allocate(Arena arena, MemoryLayout layout, int count) {
        return arena.allocateArray(layout, count);
    }

    /**
     * Allocate and copy the given string into native memory.
     */
    static MemorySegment allocateFrom(Arena arena, String str, Charset charset) {
        return arena.allocateArray(JAVA_BYTE, str.getBytes(charset));
    }

    private ArenaUtil() {}
}
