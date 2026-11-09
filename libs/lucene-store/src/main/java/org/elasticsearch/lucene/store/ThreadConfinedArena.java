/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import org.elasticsearch.core.CheckedSupplier;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * Binds a confined {@link Arena} to the current thread for the duration of an action,
 * accessible through the {@link #allocate} method. Such allocated segments must
 * only be accessed from the current thread
 */
public final class ThreadConfinedArena {

    static final ThreadLocal<Arena> CURRENT = new ThreadLocal<>();

    private ThreadConfinedArena() {}

    /**
     * Runs {@code action} with a new confined arena bound to the current thread, and closes the arena when
     * {@code action} returns. Any arena bound by an enclosing call is restored afterwards.
     */
    public static <T, E extends Exception> T withConfinedArena(CheckedSupplier<T, E> action) throws E {
        Arena previous = CURRENT.get();
        try (Arena arena = Arena.ofConfined()) {
            CURRENT.set(arena);
            try {
                return action.get();
            } finally {
                if (previous == null) {
                    CURRENT.remove();
                } else {
                    CURRENT.set(previous);
                }
            }
        }
    }

    /**
     * Allocates from the current thread-confined arena, or {@link Arena#ofAuto} if none is currently set
     */
    public static MemorySegment allocate(long byteSize, long byteAlignment) {
        return Objects.requireNonNullElseGet(CURRENT.get(), Arena::ofAuto).allocate(byteSize, byteAlignment);
    }
}
