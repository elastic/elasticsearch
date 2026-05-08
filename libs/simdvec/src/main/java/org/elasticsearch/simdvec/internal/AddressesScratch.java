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
     */
    public MemorySegment get(int count) {
        long needed = (long) count * ValueLayout.ADDRESS.byteSize();
        if (seg == null || seg.byteSize() < needed) {
            seg = Arena.ofAuto().allocate(needed, ValueLayout.ADDRESS.byteAlignment());
        }
        return seg;
    }
}
