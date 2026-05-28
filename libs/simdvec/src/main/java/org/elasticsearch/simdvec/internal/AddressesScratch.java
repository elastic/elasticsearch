/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Reusable, lazily-grown scratch buffer for an array of native addresses
 * (pointer-width values), used to hand a contiguous {@code MemorySegment}
 * of {@code MemorySegment#address()}-style values (array of pointers).
 *
 * <p>Not thread-safe; instances must not be shared across threads.
 */
public final class AddressesScratch {

    private MemorySegment seg;

    /**
     * Returns a {@link MemorySegment} of at least {@code count} native-address slots.
     * The buffer may be larger than requested (it is grown lazily and never shrunk
     * across calls); callers must respect their own {@code count} when reading or
     * writing addresses. Always returns the same backing segment for a given
     * instance until a larger one is needed.
     *
     * <p>
     * Segments are returned from an auto arena, so they are garbage-collected. Choice
     * here was between:<ul>
     * <li> a confined arena (but confined arenas have thread affinity, which is probably
     * OK for our use cases, but it would add a very implicit contract),</li>
     * <li> a shared arena (but that has extra
     * invocation costs)</li>
     * <li> or an auto arena.</li>
     * </ul>
     * The first 2 would have to be closed and re-created (basically, 1 arena - 1 segment).
     * The pro would have been a more controlled/deterministic lifecycle. The cons are listed above,
     * plus the additional complexity that an auto arena does not have. Auto here seems to be
     * the sweet spot.
     */
    public MemorySegment get(int count) {
        long needed = (long) count * ValueLayout.ADDRESS.byteSize();
        if (seg == null || seg.byteSize() < needed) {
            // No need to call close() here, or to keep a reference to the Arena: Arena#ofAuto is
            // not closeable, and returns MemorySegments whose lifetime is managed automatically by GC.
            seg = Arena.ofAuto().allocate(needed, ValueLayout.ADDRESS.byteAlignment());
        }
        return seg;
    }
}
