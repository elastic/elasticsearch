/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.lucene.store.SegmentBufferScratch;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.IntFunction;

/**
 * The query handed to vector scorers. It is immutable, so that scorers which copy the query can do so
 * only when they are handed a different instance: new query contents need a new instance, and
 * {@code bytes} must not be modified while the instance can still be passed to a scorer.
 */
public final class ScoringQuery {
    private final byte[] bytes;

    public ScoringQuery(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] bytes() {
        return bytes;
    }

    public static final class OffHeapCopy {
        private final IntFunction<MemorySegment> scratch;
        private ScoringQuery query;
        private MemorySegment segment;

        public OffHeapCopy() {
            this(new SegmentBufferScratch());
        }

        OffHeapCopy(IntFunction<MemorySegment> scratch) {
            this.scratch = scratch;
        }

        public MemorySegment segmentFor(ScoringQuery q) {
            byte[] bytes = q.bytes();
            if (q != query) {
                segment = scratch.apply(bytes.length);
                MemorySegment.copy(bytes, 0, segment, ValueLayout.JAVA_BYTE, 0, bytes.length);
                query = q;
            }
            assert MemorySegment.mismatch(segment, 0, bytes.length, MemorySegment.ofArray(bytes), 0, bytes.length) == -1
                : "query bytes changed while the query was in use";
            return segment;
        }
    }
}
