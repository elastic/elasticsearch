/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;

/**
 * Per-page {@link NumericSortKeyExtractor} for {@code DoubleBlock} sort keys.
 *
 * <p>Lucene's {@link NumericUtils#doubleToSortableLong(double)} maps doubles to longs preserving
 * IEEE-754 total order: it flips the sign bit for positives, and inverts the bit pattern for
 * negatives so that the long ordering matches the floating-point ordering across signed zeros
 * and the NaN payload. The encoded long can then be ranked by the heap like any other LONG.
 *
 * <p>For MV-min/MV-max we scan in the {@code double} domain — comparing {@code double}s gives the
 * correct numeric minimum without an extra encode-and-compare round trip, matching how
 * {@code KeyExtractorForDouble.MinFromUnorderedBlock} behaves.
 */
abstract class DoubleSortKeyExtractor implements NumericSortKeyExtractor {

    static DoubleSortKeyExtractor extractorFor(DoubleBlock block, boolean ascending) {
        DoubleVector v = block.asVector();
        if (v != null) {
            return new FromVector(v, ascending);
        }
        if (ascending) {
            return block.mvSortedAscending() ? new MinFromAscendingBlock(block) : new MinFromUnorderedBlock(block);
        }
        return block.mvSortedAscending() ? new MaxFromAscendingBlock(block) : new MaxFromUnorderedBlock(block);
    }

    private static final class FromVector extends DoubleSortKeyExtractor {
        private final DoubleVector vector;
        private final boolean ascending;

        FromVector(DoubleVector vector, boolean ascending) {
            this.vector = vector;
            this.ascending = ascending;
        }

        @Override
        public long encodedAt(int position) {
            return NumericSortKeyExtractor.encode(NumericUtils.doubleToSortableLong(vector.getDouble(position)), ascending);
        }

        @Override
        public boolean isNullAt(int position) {
            return false;
        }
    }

    private static final class MinFromAscendingBlock extends DoubleSortKeyExtractor {
        private final DoubleBlock block;

        MinFromAscendingBlock(DoubleBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            return NumericSortKeyExtractor.encode(
                NumericUtils.doubleToSortableLong(block.getDouble(block.getFirstValueIndex(position))),
                true
            );
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MaxFromAscendingBlock extends DoubleSortKeyExtractor {
        private final DoubleBlock block;

        MaxFromAscendingBlock(DoubleBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int end = block.getFirstValueIndex(position) + block.getValueCount(position) - 1;
            return NumericSortKeyExtractor.encode(NumericUtils.doubleToSortableLong(block.getDouble(end)), false);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MinFromUnorderedBlock extends DoubleSortKeyExtractor {
        private final DoubleBlock block;

        MinFromUnorderedBlock(DoubleBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            double min = block.getDouble(start);
            for (int i = start + 1; i < end; i++) {
                min = Math.min(min, block.getDouble(i));
            }
            return NumericSortKeyExtractor.encode(NumericUtils.doubleToSortableLong(min), true);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }

    private static final class MaxFromUnorderedBlock extends DoubleSortKeyExtractor {
        private final DoubleBlock block;

        MaxFromUnorderedBlock(DoubleBlock block) {
            this.block = block;
        }

        @Override
        public long encodedAt(int position) {
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            double max = block.getDouble(start);
            for (int i = start + 1; i < end; i++) {
                max = Math.max(max, block.getDouble(i));
            }
            return NumericSortKeyExtractor.encode(NumericUtils.doubleToSortableLong(max), false);
        }

        @Override
        public boolean isNullAt(int position) {
            return block.isNull(position) || block.getValueCount(position) == 0;
        }
    }
}
