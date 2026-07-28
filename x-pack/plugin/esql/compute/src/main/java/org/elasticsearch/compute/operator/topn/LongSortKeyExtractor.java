/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;

/**
 * Per-page {@link NumericSortKeyExtractor} for {@code LongBlock} sort keys. Also serves
 * {@code DATETIME} and {@code DATE_NANOS}, which both lower to {@code LongBlock} at the compute
 * layer with identity ordering against the raw long.
 *
 * <p>The five inner implementations mirror {@code KeyExtractorForLong} (lines 21–34): pick a
 * dense-vector path when the block has no nulls and no MV, otherwise pick an MV-min/max scan
 * sized to the sort direction. {@link #encodedAt(int)} returns the already-flipped long so the
 * operator's hot loop is branch-free on ASC/DESC; the bit-flip is the shared
 * {@link NumericSortKeyExtractor#encode(long, boolean)} contract.
 */
abstract class LongSortKeyExtractor implements NumericSortKeyExtractor {

    /**
     * Build the right extractor for {@code block} and the configured sort direction. The
     * dispatch matches {@code KeyExtractorForLong.extractorFor} so that {@link NumericTopNOperator}
     * sees the same MV semantics the generic {@code TopNOperator} would have applied to the same
     * page.
     */
    static LongSortKeyExtractor extractorFor(LongBlock block, boolean ascending) {
        LongVector v = block.asVector();
        if (v != null) {
            return new FromVector(v, ascending);
        }
        if (ascending) {
            return block.mvSortedAscending() ? new MinFromAscendingBlock(block) : new MinFromUnorderedBlock(block);
        }
        return block.mvSortedAscending() ? new MaxFromAscendingBlock(block) : new MaxFromUnorderedBlock(block);
    }

    private static final class FromVector extends LongSortKeyExtractor {
        private final LongVector vector;
        private final boolean ascending;

        FromVector(LongVector vector, boolean ascending) {
            this.vector = vector;
            this.ascending = ascending;
        }

        @Override
        public long encodedAt(int position) {
            return NumericSortKeyExtractor.encode(vector.getLong(position), ascending);
        }

        @Override
        public boolean isNullAt(int position) {
            return false;
        }
    }

    private static final class MinFromAscendingBlock extends LongSortKeyExtractor {
        private final LongBlock block;

        MinFromAscendingBlock(LongBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            return NumericSortKeyExtractor.encode(block.getLong(block.getFirstValueIndex(position)), true);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MaxFromAscendingBlock extends LongSortKeyExtractor {
        private final LongBlock block;

        MaxFromAscendingBlock(LongBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position) - 1;
            return NumericSortKeyExtractor.encode(block.getLong(end), false);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MinFromUnorderedBlock extends LongSortKeyExtractor {
        private final LongBlock block;

        MinFromUnorderedBlock(LongBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            long min = block.getLong(start);
            for (int i = start + 1; i < end; i++) {
                min = Math.min(min, block.getLong(i));
            }
            return NumericSortKeyExtractor.encode(min, true);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MaxFromUnorderedBlock extends LongSortKeyExtractor {
        private final LongBlock block;

        MaxFromUnorderedBlock(LongBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            long max = block.getLong(start);
            for (int i = start + 1; i < end; i++) {
                max = Math.max(max, block.getLong(i));
            }
            return NumericSortKeyExtractor.encode(max, false);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }
}
