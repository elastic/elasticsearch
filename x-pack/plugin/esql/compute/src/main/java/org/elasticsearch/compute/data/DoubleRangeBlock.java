/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.index.mapper.BlockLoader;

/**
 * Block that stores double ranges.
 */
public sealed interface DoubleRangeBlock extends Block permits DoubleRangeArrayBlock, ConstantNullBlock {

    @Override
    boolean isNull(int position);

    @Override
    DoubleRangeBlock filter(boolean mayContainDuplicates, int[] positions, int offset, int length);

    @Override
    default DoubleRangeBlock filter(boolean mayContainDuplicates, int... positions) {
        return filter(mayContainDuplicates, positions, 0, positions.length);
    }

    @Override
    DoubleRangeBlock keepMask(BooleanVector mask);

    @Override
    ReleasableIterator<? extends DoubleRangeBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    @Override
    DoubleRangeBlock expand();

    /**
     * Returns {@code true} if the given blocks are equal to each other, otherwise {@code false}.
     * Two blocks are considered equal if they have the same position count, and contain the same
     * values (including absent null values) in the same order. This definition ensures that the
     * equals method works properly across different implementations of the DoubleRangeBlock interface.
     */
    static boolean equals(DoubleRangeBlock lhs, DoubleRangeBlock rhs) {
        if (lhs == rhs) {
            return true;
        }
        if (lhs.getPositionCount() != rhs.getPositionCount()) {
            return false;
        }
        return DoubleBlock.equals(lhs.getDoubleFromBlock(), rhs.getDoubleFromBlock())
            && DoubleBlock.equals(lhs.getDoubleToBlock(), rhs.getDoubleToBlock());
    }

    static int hash(DoubleRangeBlock block) {
        final int positions = block.getPositionCount();
        int ret = 1;
        for (int pos = 0; pos < positions; pos++) {
            if (block.isNull(pos)) {
                ret = 31 * ret - 1;
            } else {
                final int valueCount = block.getValueCount(pos);
                ret = 31 * ret + valueCount;
                final int firstValueIdx = block.getFirstValueIndex(pos);
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    ret *= 31;
                    ret += extractHashFrom(block.getDoubleFromBlock(), firstValueIdx, valueIndex);
                    ret *= 31;
                    ret += extractHashFrom(block.getDoubleToBlock(), firstValueIdx, valueIndex);
                }
            }
        }
        return ret;
    }

    private static int extractHashFrom(DoubleBlock b, int firstValueIdx, int valueIndex) {
        return b.isNull(firstValueIdx + valueIndex) ? -1 : Double.hashCode(b.getDouble(firstValueIdx + valueIndex));
    }

    DoubleBlock getDoubleFromBlock();

    DoubleBlock getDoubleToBlock();

    /**
     * Returns the double range at the given value index, mutating {@code scratch} in place.
     *
     * @param valueIndex the value-offset (use {@link #getFirstValueIndex(int)} to translate from a position)
     * @param scratch    the reusable container to populate
     * @return {@code scratch}, populated with {@code from}/{@code to} for the value at {@code valueIndex}
     */
    DoubleRangeBlockBuilder.DoubleRange getDoubleRange(int valueIndex, DoubleRangeBlockBuilder.DoubleRange scratch);

    /**
     * Builder for {@link DoubleRangeBlock}.
     */
    sealed interface Builder extends Block.Builder, BlockLoader.DoubleRangeBuilder permits DoubleRangeBlockBuilder {

        /**
         * Append the given range to this builder.
         */
        Block.Builder appendDoubleRange(DoubleRangeBlockBuilder.DoubleRange range);

        /**
         * Append the given range to this builder.
         */
        Block.Builder appendDoubleRange(double from, double to);

        /**
         * Copy the value(s) at the given position of {@code block} into this builder.
         */
        DoubleRangeBlock.Builder copyFrom(DoubleRangeBlock block, int position);

        @Override
        DoubleRangeBlock build();
    }
}
