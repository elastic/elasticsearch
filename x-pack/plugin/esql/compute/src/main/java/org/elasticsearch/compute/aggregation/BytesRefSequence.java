/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;

/**
 * Read-only view of an ordered sequence of {@link BytesRef} values. Used to
 * expose per-partition values from {@link BytesRefArrayState} without
 * materializing an intermediate {@code BytesRef[]} array.
 * <p>
 * The returned reference from {@link #get} may alias internal storage; callers
 * must not hold it past the next {@code get} call.
 */
sealed interface BytesRefSequence permits BytesRefSequence.Flat, BytesRefSequence.Paged {
    int size();

    BytesRef get(int index, BytesRef dest);

    /**
     * Flat implementation backed by a contiguous {@code byte[]} buffer with an
     * end-offset {@code int[]} index. Matches the layout of
     * {@link BytesRefArrayState}'s flat partition state.
     */
    final class Flat implements BytesRefSequence {
        private final byte[] data;
        private final int[] offsets;
        private final int count;

        Flat(byte[] data, int[] offsets, int count) {
            this.data = data;
            this.offsets = offsets;
            this.count = count;
        }

        @Override
        public int size() {
            return count;
        }

        @Override
        public BytesRef get(int index, BytesRef dest) {
            dest.bytes = data;
            dest.offset = offsets[index];
            dest.length = offsets[index + 1] - offsets[index];
            return dest;
        }
    }

    /**
     * Paged implementation backed by a {@link BytesRefArray}. Matches the layout
     * of {@link BytesRefArrayState}'s paged partition state.
     */
    final class Paged implements BytesRefSequence {
        private final BytesRefArray array;

        Paged(BytesRefArray array) {
            this.array = array;
        }

        @Override
        public int size() {
            return (int) array.size();
        }

        @Override
        public BytesRef get(int index, BytesRef dest) {
            return array.get(index, dest);
        }
    }
}
