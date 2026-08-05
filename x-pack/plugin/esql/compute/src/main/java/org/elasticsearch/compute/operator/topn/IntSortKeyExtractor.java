/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;

/**
 * Per-page {@link NumericSortKeyExtractor} for {@code IntBlock} sort keys.
 *
 * <p>Widening to {@code long} is a plain signed extension: the low 32 bits are unchanged and the
 * high bits replicate the sign, which preserves total order. The heap can therefore rank the
 * widened value with the shared {@link NumericSortKeyExtractor#encode(long, boolean)} routine.
 */
abstract class IntSortKeyExtractor implements NumericSortKeyExtractor {

    static IntSortKeyExtractor extractorFor(IntBlock block, boolean ascending) {
        IntVector v = block.asVector();
        if (v != null) {
            return new FromVector(v, ascending);
        }
        if (ascending) {
            return block.mvSortedAscending() ? new MinFromAscendingBlock(block) : new MinFromUnorderedBlock(block);
        }
        return block.mvSortedAscending() ? new MaxFromAscendingBlock(block) : new MaxFromUnorderedBlock(block);
    }

    private static final class FromVector extends IntSortKeyExtractor {
        private final IntVector vector;
        private final boolean ascending;

        FromVector(IntVector vector, boolean ascending) {
            this.vector = vector;
            this.ascending = ascending;
        }

        @Override
        public long encodedAt(int position) {
            return NumericSortKeyExtractor.encode(vector.getInt(position), ascending);
        }

        @Override
        public boolean isNullAt(int position) {
            return false;
        }
    }

    private static final class MinFromAscendingBlock extends IntSortKeyExtractor {
        private final IntBlock block;

        MinFromAscendingBlock(IntBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            return NumericSortKeyExtractor.encode(block.getInt(block.getFirstValueIndex(position)), true);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MaxFromAscendingBlock extends IntSortKeyExtractor {
        private final IntBlock block;

        MaxFromAscendingBlock(IntBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int end = block.getFirstValueIndex(position) + block.getValueCount(position) - 1;
            return NumericSortKeyExtractor.encode(block.getInt(end), false);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MinFromUnorderedBlock extends IntSortKeyExtractor {
        private final IntBlock block;

        MinFromUnorderedBlock(IntBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            int min = block.getInt(start);
            for (int i = start + 1; i < end; i++) {
                min = Math.min(min, block.getInt(i));
            }
            return NumericSortKeyExtractor.encode(min, true);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MaxFromUnorderedBlock extends IntSortKeyExtractor {
        private final IntBlock block;

        MaxFromUnorderedBlock(IntBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            int max = block.getInt(start);
            for (int i = start + 1; i < end; i++) {
                max = Math.max(max, block.getInt(i));
            }
            return NumericSortKeyExtractor.encode(max, false);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }
}
