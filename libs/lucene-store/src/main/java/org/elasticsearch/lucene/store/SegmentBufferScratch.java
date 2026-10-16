/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.IntFunction;

/**
 * Reusable, lazily-grown off-heap scratch buffer. Holding one instance
 * avoids allocating on every call.
 *
 * <p>Not thread-safe; instances must not be shared across threads.
 */
public final class SegmentBufferScratch implements IntFunction<MemorySegment> {

    private MemorySegment backing;
    private MemorySegment slice;

    /**
     * Returns a {@link MemorySegment} of exactly {@code count} bytes, aligned for
     * any primitive layout. The backing allocation is grown lazily and never shrunk,
     * so a run of equally-sized requests allocates nothing at all.
     *
     * <p>Segments are returned from an auto arena, so they are garbage-collected.
     */
    @Override
    public MemorySegment apply(int count) {
        if (slice == null || slice.byteSize() != count) {
            if (backing == null || backing.byteSize() < count) {
                backing = Arena.ofAuto().allocate(count, ValueLayout.JAVA_LONG.byteAlignment());
            }
            slice = backing.asSlice(0, count);
        }
        return slice;
    }
}
