/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.Locale;

/**
 * Extracts sort keys for top-n from their {@link IntBlock}s.
 * This class is generated. Edit {@code X-KeyExtractor.java.st} instead.
 */
abstract class KeyExtractorForInt implements KeyExtractor {
    static KeyExtractorForInt extractorFor(TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, IntBlock block) {
        IntVector v = block.asVector();
        if (v != null) {
            return new KeyExtractorForInt.FromVector(encoder, nul, nonNul, v);
        }
        if (ascending) {
            return block.mvSortedAscending()
                ? new KeyExtractorForInt.MinFromAscendingBlock(encoder, nul, nonNul, block)
                : new KeyExtractorForInt.MinFromUnorderedBlock(encoder, nul, nonNul, block);
        }
        return block.mvSortedAscending()
            ? new KeyExtractorForInt.MaxFromAscendingBlock(encoder, nul, nonNul, block)
            : new KeyExtractorForInt.MaxFromUnorderedBlock(encoder, nul, nonNul, block);
    }

    private final byte nul;
    private final byte nonNul;

    KeyExtractorForInt(TopNEncoder encoder, byte nul, byte nonNul) {
        assert encoder == TopNEncoder.DEFAULT_SORTABLE;
        this.nul = nul;
        this.nonNul = nonNul;
    }

    protected final int nonNul(BreakingBytesRefBuilder key, int value) {
        key.append(nonNul);
        TopNEncoder.DEFAULT_SORTABLE.encodeInt(value, key);
        return Integer.BYTES + 1;
    }

    protected final int nul(BreakingBytesRefBuilder key) {
        key.append(nul);
        return 1;
    }

    @Override
    public final String toString() {
        return String.format(Locale.ROOT, "KeyExtractorForInt%s(%s, %s)", getClass().getSimpleName(), nul, nonNul);
    }

    static class FromVector extends KeyExtractorForInt {
        private final IntVector vector;

        FromVector(TopNEncoder encoder, byte nul, byte nonNul, IntVector vector) {
            super(encoder, nul, nonNul);
            this.vector = vector;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            return nonNul(key, vector.getInt(position));
        }
    }

    static class MinFromAscendingBlock extends KeyExtractorForInt {
        private final IntBlock block;

        MinFromAscendingBlock(TopNEncoder encoder, byte nul, byte nonNul, IntBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getInt(block.getFirstValueIndex(position)));
        }
    }

    static class MaxFromAscendingBlock extends KeyExtractorForInt {
        private final IntBlock block;

        MaxFromAscendingBlock(TopNEncoder encoder, byte nul, byte nonNul, IntBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getInt(block.getFirstValueIndex(position) + block.getValueCount(position) - 1));
        }
    }

    static class MinFromUnorderedBlock extends KeyExtractorForInt {
        private final IntBlock block;

        MinFromUnorderedBlock(TopNEncoder encoder, byte nul, byte nonNul, IntBlock block) {
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
            int min = block.getInt(start);
            for (int i = start + 1; i < end; i++) {
                min = Math.min(min, block.getInt(i));
            }
            return nonNul(key, min);
        }
    }

    static class MaxFromUnorderedBlock extends KeyExtractorForInt {
        private final IntBlock block;

        MaxFromUnorderedBlock(TopNEncoder encoder, byte nul, byte nonNul, IntBlock block) {
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
            int max = block.getInt(start);
            for (int i = start + 1; i < end; i++) {
                max = Math.max(max, block.getInt(i));
            }
            return nonNul(key, max);
        }
    }
}
