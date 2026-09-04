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

import java.util.Locale;

/**
 * Extracts sort keys for top-n from their {@link BooleanBlock}s.
 * This class is generated. Edit {@code X-KeyExtractor.java.st} instead.
 */
abstract class KeyExtractorForBoolean implements KeyExtractor {
    static KeyExtractorForBoolean extractorFor(TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, BooleanBlock block) {
        BooleanVector v = block.asVector();
        if (v != null) {
            return new KeyExtractorForBoolean.FromVector(encoder, nul, nonNul, v);
        }
        if (ascending) {
            return block.mvSortedAscending()
                ? new KeyExtractorForBoolean.MinFromAscendingBlock(encoder, nul, nonNul, block)
                : new KeyExtractorForBoolean.MinFromUnorderedBlock(encoder, nul, nonNul, block);
        }
        return block.mvSortedAscending()
            ? new KeyExtractorForBoolean.MaxFromAscendingBlock(encoder, nul, nonNul, block)
            : new KeyExtractorForBoolean.MaxFromUnorderedBlock(encoder, nul, nonNul, block);
    }

    private final TopNEncoder encoder;
    private final byte nul;
    private final byte nonNul;

    KeyExtractorForBoolean(TopNEncoder encoder, byte nul, byte nonNul) {
        this.encoder = encoder;
        this.nul = nul;
        this.nonNul = nonNul;
    }

    protected final void nonNul(BreakingBytesRefBuilder key, boolean value) {
        key.append(nonNul);
        encoder.encodeBoolean(value, key);
    }

    protected final void nul(BreakingBytesRefBuilder key) {
        key.append(nul);
    }

    @Override
    public final String toString() {
        return String.format(Locale.ROOT, "KeyExtractorForBoolean%s(%s, %s)", getClass().getSimpleName(), nul, nonNul);
    }

    static class FromVector extends KeyExtractorForBoolean {
        private final BooleanVector vector;

        FromVector(TopNEncoder encoder, byte nul, byte nonNul, BooleanVector vector) {
            super(encoder, nul, nonNul);
            this.vector = vector;
        }

        @Override
        public void writeKey(BreakingBytesRefBuilder key, int position) {
            nonNul(key, vector.getBoolean(position));
        }
    }

    static class MinFromAscendingBlock extends KeyExtractorForBoolean {
        private final BooleanBlock block;

        MinFromAscendingBlock(TopNEncoder encoder, byte nul, byte nonNul, BooleanBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public void writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                nul(key);
                return;
            }
            nonNul(key, block.getBoolean(block.getFirstValueIndex(position)));
        }
    }

    static class MaxFromAscendingBlock extends KeyExtractorForBoolean {
        private final BooleanBlock block;

        MaxFromAscendingBlock(TopNEncoder encoder, byte nul, byte nonNul, BooleanBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public void writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                nul(key);
                return;
            }
            nonNul(key, block.getBoolean(block.getFirstValueIndex(position) + block.getValueCount(position) - 1));
        }
    }

    static class MinFromUnorderedBlock extends KeyExtractorForBoolean {
        private final BooleanBlock block;

        MinFromUnorderedBlock(TopNEncoder encoder, byte nul, byte nonNul, BooleanBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public void writeKey(BreakingBytesRefBuilder key, int position) {
            int size = block.getValueCount(position);
            if (size == 0) {
                nul(key);
                return;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            for (int i = start; i < end; i++) {
                if (block.getBoolean(i) == false) {
                    nonNul(key, false);
                    return;
                }
            }
            nonNul(key, true);
        }
    }

    static class MaxFromUnorderedBlock extends KeyExtractorForBoolean {
        private final BooleanBlock block;

        MaxFromUnorderedBlock(TopNEncoder encoder, byte nul, byte nonNul, BooleanBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public void writeKey(BreakingBytesRefBuilder key, int position) {
            int size = block.getValueCount(position);
            if (size == 0) {
                nul(key);
                return;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            for (int i = start; i < end; i++) {
                if (block.getBoolean(i)) {
                    nonNul(key, true);
                    return;
                }
            }
            nonNul(key, false);
        }
    }
}
