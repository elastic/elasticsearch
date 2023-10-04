/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

abstract class KeyExtractorForBoolean implements KeyExtractor {
    static KeyExtractorForBoolean extractorFor(TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, BooleanBlock block) {
        BooleanVector v = block.asVector();
        if (v != null) {
            return new KeyExtractorForBoolean.ForVector(encoder, nul, nonNul, v);
        }
        if (ascending) {
            return block.mvSortedAscending()
                ? new KeyExtractorForBoolean.MinForAscending(encoder, nul, nonNul, block)
                : new KeyExtractorForBoolean.MinForUnordered(encoder, nul, nonNul, block);
        }
        return block.mvSortedAscending()
            ? new KeyExtractorForBoolean.MaxForAscending(encoder, nul, nonNul, block)
            : new KeyExtractorForBoolean.MaxForUnordered(encoder, nul, nonNul, block);
    }

    private final byte nul;
    private final byte nonNul;

    KeyExtractorForBoolean(TopNEncoder encoder, byte nul, byte nonNul) {
        assert encoder == TopNEncoder.DEFAULT_SORTABLE;
        this.nul = nul;
        this.nonNul = nonNul;
    }

    protected final int nonNul(BreakingBytesRefBuilder key, boolean value) {
        key.append(nonNul);
        TopNEncoder.DEFAULT_SORTABLE.encodeBoolean(value, key);
        return Byte.BYTES + 1;
    }

    protected final int nul(BreakingBytesRefBuilder key) {
        key.append(nul);
        return 1;
    }

    static class ForVector extends KeyExtractorForBoolean {
        private final BooleanVector vector;

        ForVector(TopNEncoder encoder, byte nul, byte nonNul, BooleanVector vector) {
            super(encoder, nul, nonNul);
            this.vector = vector;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            return nonNul(key, vector.getBoolean(position));
        }
    }

    static class MinForAscending extends KeyExtractorForBoolean {
        private final BooleanBlock block;

        MinForAscending(TopNEncoder encoder, byte nul, byte nonNul, BooleanBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getBoolean(block.getFirstValueIndex(position)));
        }
    }

    static class MaxForAscending extends KeyExtractorForBoolean {
        private final BooleanBlock block;

        MaxForAscending(TopNEncoder encoder, byte nul, byte nonNul, BooleanBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getBoolean(block.getFirstValueIndex(position) + block.getValueCount(position) - 1));
        }
    }

    static class MinForUnordered extends KeyExtractorForBoolean {
        private final BooleanBlock block;

        MinForUnordered(TopNEncoder encoder, byte nul, byte nonNul, BooleanBlock block) {
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
            for (int i = start; i < end; i++) {
                if (block.getBoolean(i) == false) {
                    return nonNul(key, false);
                }
            }
            return nonNul(key, true);
        }
    }

    static class MaxForUnordered extends KeyExtractorForBoolean {
        private final BooleanBlock block;

        MaxForUnordered(TopNEncoder encoder, byte nul, byte nonNul, BooleanBlock block) {
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
            for (int i = start; i < end; i++) {
                if (block.getBoolean(i)) {
                    return nonNul(key, true);
                }
            }
            return nonNul(key, false);
        }
    }
}
