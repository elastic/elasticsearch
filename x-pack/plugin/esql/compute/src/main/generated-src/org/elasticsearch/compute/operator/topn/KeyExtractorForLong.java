/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

abstract class KeyExtractorForLong implements KeyExtractor {
    static KeyExtractorForLong extractorFor(TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, LongBlock block) {
        LongVector v = block.asVector();
        if (v != null) {
            return new KeyExtractorForLong.ForVector(encoder, nul, nonNul, v);
        }
        if (ascending) {
            return block.mvSortedAscending()
                ? new KeyExtractorForLong.MinForAscending(encoder, nul, nonNul, block)
                : new KeyExtractorForLong.MinForUnordered(encoder, nul, nonNul, block);
        }
        return block.mvSortedAscending()
            ? new KeyExtractorForLong.MaxForAscending(encoder, nul, nonNul, block)
            : new KeyExtractorForLong.MaxForUnordered(encoder, nul, nonNul, block);
    }

    private final byte nul;
    private final byte nonNul;

    KeyExtractorForLong(TopNEncoder encoder, byte nul, byte nonNul) {
        assert encoder == TopNEncoder.DEFAULT_SORTABLE;
        this.nul = nul;
        this.nonNul = nonNul;
    }

    protected final int nonNul(BreakingBytesRefBuilder key, long value) {
        key.append(nonNul);
        TopNEncoder.DEFAULT_SORTABLE.encodeLong(value, key);
        return Long.BYTES + 1;
    }

    protected final int nul(BreakingBytesRefBuilder key) {
        key.append(nul);
        return 1;
    }

    static class ForVector extends KeyExtractorForLong {
        private final LongVector vector;

        ForVector(TopNEncoder encoder, byte nul, byte nonNul, LongVector vector) {
            super(encoder, nul, nonNul);
            this.vector = vector;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            return nonNul(key, vector.getLong(position));
        }
    }

    static class MinForAscending extends KeyExtractorForLong {
        private final LongBlock block;

        MinForAscending(TopNEncoder encoder, byte nul, byte nonNul, LongBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getLong(block.getFirstValueIndex(position)));
        }
    }

    static class MaxForAscending extends KeyExtractorForLong {
        private final LongBlock block;

        MaxForAscending(TopNEncoder encoder, byte nul, byte nonNul, LongBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getLong(block.getFirstValueIndex(position) + block.getValueCount(position) - 1));
        }
    }

    static class MinForUnordered extends KeyExtractorForLong {
        private final LongBlock block;

        MinForUnordered(TopNEncoder encoder, byte nul, byte nonNul, LongBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            int size = block.getValueCount(position);
            if (size == 0) {
                return nul(key);
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            long min = block.getLong(start);
            for (int i = start + 1; i < end; i++) {
                min = Math.min(min, block.getLong(i));
            }
            return nonNul(key, min);
        }
    }

    static class MaxForUnordered extends KeyExtractorForLong {
        private final LongBlock block;

        MaxForUnordered(TopNEncoder encoder, byte nul, byte nonNul, LongBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            int size = block.getValueCount(position);
            if (size == 0) {
                return nul(key);
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            long max = block.getLong(start);
            for (int i = start + 1; i < end; i++) {
                max = Math.max(max, block.getLong(i));
            }
            return nonNul(key, max);
        }
    }
}
