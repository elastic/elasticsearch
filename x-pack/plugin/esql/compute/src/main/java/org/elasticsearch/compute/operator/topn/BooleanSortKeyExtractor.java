/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;

/**
 * Per-page {@link NumericSortKeyExtractor} for {@code BooleanBlock} sort keys.
 *
 * <p>Encoding is the trivial map {@code false → 0L, true → 1L}, which matches ESQL's documented
 * boolean sort order ({@code false} sorts before {@code true} under ASC). The heap then ranks
 * the widened long with the same flip used for other types.
 *
 * <p>MV-min ASC short-circuits as soon as it sees {@code false} (there's no value to find that
 * sorts earlier); MV-max DESC short-circuits on {@code true}. This is purely a hot-loop speedup
 * for wide MV lists and produces the same answer as the full scan.
 */
abstract class BooleanSortKeyExtractor implements NumericSortKeyExtractor {

    static BooleanSortKeyExtractor extractorFor(BooleanBlock block, boolean ascending) {
        BooleanVector v = block.asVector();
        if (v != null) {
            return new FromVector(v, ascending);
        }
        if (ascending) {
            return block.mvSortedAscending() ? new MinFromAscendingBlock(block) : new MinFromUnorderedBlock(block);
        }
        return block.mvSortedAscending() ? new MaxFromAscendingBlock(block) : new MaxFromUnorderedBlock(block);
    }

    private static long encode(boolean raw, boolean ascending) {
        return NumericSortKeyExtractor.encode(raw ? 1L : 0L, ascending);
    }

    private static final class FromVector extends BooleanSortKeyExtractor {
        private final BooleanVector vector;
        private final boolean ascending;

        FromVector(BooleanVector vector, boolean ascending) {
            this.vector = vector;
            this.ascending = ascending;
        }

        @Override
        public long encodedAt(int position) {
            return encode(vector.getBoolean(position), ascending);
        }

        @Override
        public boolean isNullAt(int position) {
            return false;
        }
    }

    private static final class MinFromAscendingBlock extends BooleanSortKeyExtractor {
        private final BooleanBlock block;

        MinFromAscendingBlock(BooleanBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            return encode(block.getBoolean(block.getFirstValueIndex(position)), true);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MaxFromAscendingBlock extends BooleanSortKeyExtractor {
        private final BooleanBlock block;

        MaxFromAscendingBlock(BooleanBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int end = block.getFirstValueIndex(position) + block.getValueCount(position) - 1;
            return encode(block.getBoolean(end), false);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MinFromUnorderedBlock extends BooleanSortKeyExtractor {
        private final BooleanBlock block;

        MinFromUnorderedBlock(BooleanBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                if (block.getBoolean(i) == false) {
                    return encode(false, true);
                }
            }
            return encode(true, true);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MaxFromUnorderedBlock extends BooleanSortKeyExtractor {
        private final BooleanBlock block;

        MaxFromUnorderedBlock(BooleanBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                if (block.getBoolean(i)) {
                    return encode(true, false);
                }
            }
            return encode(false, false);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }
}
