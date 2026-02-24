/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

/**
 * Block that stores long ranges.
 */
public sealed interface LongRangeBlock extends Block permits LongRangeArrayBlock, ConstantNullBlock {
    @Override
    LongRangeBlock filter(boolean mayContainDuplicates, int... positions);

    @Override
    LongRangeBlock keepMask(BooleanVector mask);

    @Override
    ReleasableIterator<? extends LongRangeBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    @Override
    LongRangeBlock expand();

    /**
     * Returns {@code true} if the given blocks are equal to each other, otherwise {@code false}.
     * Two blocks are considered equal if they have the same position count, and contain the same
     * values (including absent null values) in the same order. This definition ensures that the
     * equals method works properly across different implementations of the LongRangeBlock interface.
     */
    static boolean equals(LongRangeBlock lhs, LongRangeBlock rhs) {
        if (lhs == rhs) {
            return true;
        }
        if (lhs.getPositionCount() != rhs.getPositionCount()) {
            return false;
        }
        return LongBlock.equals(lhs.getFromBlock(), rhs.getFromBlock()) && LongBlock.equals(lhs.getToBlock(), rhs.getToBlock());
    }

    static int hash(LongRangeBlock block) {
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
                    ret += extractHashFrom(block.getFromBlock(), firstValueIdx, valueIndex);
                    ret *= 31;
                    ret += extractHashFrom(block.getToBlock(), firstValueIdx, valueIndex);
                }
            }
        }
        return ret;
    }

    private static int extractHashFrom(LongBlock b, int firstValueIdx, int valueIndex) {
        return b.isNull(firstValueIdx + valueIndex) ? -1 : Long.hashCode(b.getLong(firstValueIdx + valueIndex));
    }

    // TODO: those DateRange-specific sub-block getters, together with the AggregateMetricBuilder specific getters
    // Should probably be refactored into some "composite block" interface, to avoid the need to implement all of
    // them in ConstantArrayBlock. Something like `Block getSubBlock(int index)` would be enough.
    // I think that was the original intent of CompositeBlock - not sure why it was abandoned.
    LongBlock getFromBlock();

    LongBlock getToBlock();
}
