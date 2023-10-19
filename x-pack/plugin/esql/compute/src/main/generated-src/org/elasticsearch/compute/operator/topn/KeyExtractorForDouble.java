/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

abstract class KeyExtractorForDouble implements KeyExtractor {
    static KeyExtractorForDouble extractorFor(TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, DoubleBlock block) {
        DoubleVector v = block.asVector();
        if (v != null) {
            return new KeyExtractorForDouble.ForVector(encoder, nul, nonNul, v);
        }
        if (ascending) {
            return block.mvSortedAscending()
                ? new KeyExtractorForDouble.MinForAscending(encoder, nul, nonNul, block)
                : new KeyExtractorForDouble.MinForUnordered(encoder, nul, nonNul, block);
        }
        return block.mvSortedAscending()
            ? new KeyExtractorForDouble.MaxForAscending(encoder, nul, nonNul, block)
            : new KeyExtractorForDouble.MaxForUnordered(encoder, nul, nonNul, block);
    }

    private final byte nul;
    private final byte nonNul;

    KeyExtractorForDouble(TopNEncoder encoder, byte nul, byte nonNul) {
        assert encoder == TopNEncoder.DEFAULT_SORTABLE;
        this.nul = nul;
        this.nonNul = nonNul;
    }

    protected final int nonNul(BreakingBytesRefBuilder key, double value) {
        key.append(nonNul);
        TopNEncoder.DEFAULT_SORTABLE.encodeDouble(value, key);
        return Double.BYTES + 1;
    }

    protected final int nul(BreakingBytesRefBuilder key) {
        key.append(nul);
        return 1;
    }

    static class ForVector extends KeyExtractorForDouble {
        private final DoubleVector vector;

        ForVector(TopNEncoder encoder, byte nul, byte nonNul, DoubleVector vector) {
            super(encoder, nul, nonNul);
            this.vector = vector;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            return nonNul(key, vector.getDouble(position));
        }
    }

    static class MinForAscending extends KeyExtractorForDouble {
        private final DoubleBlock block;

        MinForAscending(TopNEncoder encoder, byte nul, byte nonNul, DoubleBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getDouble(block.getFirstValueIndex(position)));
        }
    }

    static class MaxForAscending extends KeyExtractorForDouble {
        private final DoubleBlock block;

        MaxForAscending(TopNEncoder encoder, byte nul, byte nonNul, DoubleBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getDouble(block.getFirstValueIndex(position) + block.getValueCount(position) - 1));
        }
    }

    static class MinForUnordered extends KeyExtractorForDouble {
        private final DoubleBlock block;

        MinForUnordered(TopNEncoder encoder, byte nul, byte nonNul, DoubleBlock block) {
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
            double min = block.getDouble(start);
            for (int i = start + 1; i < end; i++) {
                min = Math.min(min, block.getDouble(i));
            }
            return nonNul(key, min);
        }
    }

    static class MaxForUnordered extends KeyExtractorForDouble {
        private final DoubleBlock block;

        MaxForUnordered(TopNEncoder encoder, byte nul, byte nonNul, DoubleBlock block) {
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
            double max = block.getDouble(start);
            for (int i = start + 1; i < end; i++) {
                max = Math.max(max, block.getDouble(i));
            }
            return nonNul(key, max);
        }
    }
}
