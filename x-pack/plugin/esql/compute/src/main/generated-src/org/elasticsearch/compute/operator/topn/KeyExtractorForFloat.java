/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.Locale;

/**
 * Extracts sort keys for top-n from their {@link FloatBlock}s.
 * This class is generated. Edit {@code X-KeyExtractor.java.st} instead.
 */
abstract class KeyExtractorForFloat implements KeyExtractor {
    static KeyExtractorForFloat extractorFor(TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, FloatBlock block) {
        FloatVector v = block.asVector();
        if (v != null) {
            return new KeyExtractorForFloat.FromVector(encoder, nul, nonNul, v);
        }
        if (ascending) {
            return block.mvSortedAscending()
                ? new KeyExtractorForFloat.MinFromAscendingBlock(encoder, nul, nonNul, block)
                : new KeyExtractorForFloat.MinFromUnorderedBlock(encoder, nul, nonNul, block);
        }
        return block.mvSortedAscending()
            ? new KeyExtractorForFloat.MaxFromAscendingBlock(encoder, nul, nonNul, block)
            : new KeyExtractorForFloat.MaxFromUnorderedBlock(encoder, nul, nonNul, block);
    }

    private final byte nul;
    private final byte nonNul;

    KeyExtractorForFloat(TopNEncoder encoder, byte nul, byte nonNul) {
        assert encoder == TopNEncoder.DEFAULT_SORTABLE;
        this.nul = nul;
        this.nonNul = nonNul;
    }

    protected final int nonNul(BreakingBytesRefBuilder key, float value) {
        key.append(nonNul);
        TopNEncoder.DEFAULT_SORTABLE.encodeFloat(value, key);
        return Float.BYTES + 1;
    }

    protected final int nul(BreakingBytesRefBuilder key) {
        key.append(nul);
        return 1;
    }

    @Override
    public final String toString() {
        return String.format(Locale.ROOT, "KeyExtractorForFloat%s(%s, %s)", getClass().getSimpleName(), nul, nonNul);
    }

    static class FromVector extends KeyExtractorForFloat {
        private final FloatVector vector;

        FromVector(TopNEncoder encoder, byte nul, byte nonNul, FloatVector vector) {
            super(encoder, nul, nonNul);
            this.vector = vector;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            return nonNul(key, vector.getFloat(position));
        }
    }

    static class MinFromAscendingBlock extends KeyExtractorForFloat {
        private final FloatBlock block;

        MinFromAscendingBlock(TopNEncoder encoder, byte nul, byte nonNul, FloatBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getFloat(block.getFirstValueIndex(position)));
        }
    }

    static class MaxFromAscendingBlock extends KeyExtractorForFloat {
        private final FloatBlock block;

        MaxFromAscendingBlock(TopNEncoder encoder, byte nul, byte nonNul, FloatBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getFloat(block.getFirstValueIndex(position) + block.getValueCount(position) - 1));
        }
    }

    static class MinFromUnorderedBlock extends KeyExtractorForFloat {
        private final FloatBlock block;

        MinFromUnorderedBlock(TopNEncoder encoder, byte nul, byte nonNul, FloatBlock block) {
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
            float min = block.getFloat(start);
            for (int i = start + 1; i < end; i++) {
                min = Math.min(min, block.getFloat(i));
            }
            return nonNul(key, min);
        }
    }

    static class MaxFromUnorderedBlock extends KeyExtractorForFloat {
        private final FloatBlock block;

        MaxFromUnorderedBlock(TopNEncoder encoder, byte nul, byte nonNul, FloatBlock block) {
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
            float max = block.getFloat(start);
            for (int i = start + 1; i < end; i++) {
                max = Math.max(max, block.getFloat(i));
            }
            return nonNul(key, max);
        }
    }
}
