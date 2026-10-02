/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.adapter;

import org.elasticsearch.core.CheckedFunction;

import java.lang.foreign.MemorySegment;

/**
 * {@link MemorySegment} operations whose behaviour differs between JDK 21 and 22+.
 *
 * <p>This is the JDK 22+ implementation of this multi-release jar.
 */
public final class MemorySegmentUtils {

    private MemorySegmentUtils() {}

    /**
     * Applies {@code action} to a memory segment holding {@code array[0, length)} that is valid as
     * an argument to a native downcall.
     *
     * <p>On JDK 22+ the array is wrapped in place and no copy is made.
     *
     * <p>The segment is valid only for the duration of the call; callers must not retain it.
     *
     * @param array  the source bytes; may be longer than {@code length}, in which case only the
     *               leading {@code length} bytes are exposed
     * @param length the number of bytes in {@code array} to expose as a {@link MemorySegment}
     * @param action the action to apply to the segment
     * @return the result of applying {@code action}
     */
    public static <R, E extends Exception> R withDowncallSegment(byte[] array, int length, CheckedFunction<MemorySegment, R, E> action)
        throws E {
        return action.apply(MemorySegment.ofArray(array).asSlice(0, length));
    }
}
