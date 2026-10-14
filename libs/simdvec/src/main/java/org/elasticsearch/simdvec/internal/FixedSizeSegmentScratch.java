/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.elasticsearch.core.Strings;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.IntFunction;

/**
 * Reusable off-heap scratch buffer of a single, fixed size, known when the owning
 * scorer is constructed. Rejects any other size rather than growing.
 *
 * <p>Not thread-safe; instances must not be shared across threads.
 */
public class FixedSizeSegmentScratch implements IntFunction<MemorySegment> {
    private final int size;
    private MemorySegment scratch;

    public FixedSizeSegmentScratch(int size) {
        this.size = size;
    }

    /**
     * Returns a {@link MemorySegment} of exactly {@code len} bytes, aligned for any
     * primitive layout, allocated on first use.
     *
     * <p>Segments are returned from an auto arena, so they are garbage-collected.
     *
     * @throws IllegalArgumentException if {@code len} is not the size this instance was created with
     */
    @Override
    public MemorySegment apply(int len) {
        if (len != size) {
            throw new IllegalArgumentException(
                Strings.format("Illegal size: FixedSizeSegmentScratch has a size of %d, requested %d", size, len)
            );
        }
        if (scratch == null) {
            scratch = Arena.ofAuto().allocate(size, ValueLayout.JAVA_LONG.byteAlignment());
        }
        return scratch;
    }
}
