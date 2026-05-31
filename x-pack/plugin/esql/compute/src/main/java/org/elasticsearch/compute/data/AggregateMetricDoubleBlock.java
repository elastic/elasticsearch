/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.util.List;

/**
 * Block that stores aggregate_metric_double values.
 */
public sealed interface AggregateMetricDoubleBlock extends Block permits AggregateMetricDoubleArrayBlock, ConstantNullBlock {

    @Override
    AggregateMetricDoubleBlock filter(boolean mayContainDuplicates, int... positions);

    @Override
    AggregateMetricDoubleBlock keepMask(BooleanVector mask);

    @Override
    ReleasableIterator<? extends AggregateMetricDoubleBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    @Override
    AggregateMetricDoubleBlock expand();

    /**
     * Compares the given object with this block for equality. Returns {@code true} if and only if the
     * given object is a AggregateMetricDoubleBlock, and both blocks are
     * {@link #equals(AggregateMetricDoubleBlock, AggregateMetricDoubleBlock) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this block, as defined by {@link #hash(AggregateMetricDoubleBlock)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given blocks are equal to each other, otherwise {@code false}.
     * Two blocks are considered equal if they have the same position count, and contain the same
     * values (including absent null values) in the same order. This definition ensures that the
     * equals method works properly across different implementations of the AggregateMetricDoubleBlock interface.
     */
    static boolean equals(AggregateMetricDoubleBlock block1, AggregateMetricDoubleBlock block2) {
        if (block1 == block2) {
            return true;
        }
        final int positions = block1.getPositionCount();
        if (positions != block2.getPositionCount()) {
            return false;
        }
        for (var doubleMetric : List.of(
            AggregateMetricDoubleBlockBuilder.Metric.MIN,
            AggregateMetricDoubleBlockBuilder.Metric.MAX,
            AggregateMetricDoubleBlockBuilder.Metric.SUM
        )) {
            DoubleBlock doubleBlock1 = (DoubleBlock) block1.getMetricBlock(doubleMetric.getIndex());
            DoubleBlock doubleBlock2 = (DoubleBlock) block2.getMetricBlock(doubleMetric.getIndex());
            if (DoubleBlock.equals(doubleBlock1, doubleBlock2) == false) {
                return false;
            }
        }
        IntBlock intBlock1 = block1.countBlock();
        IntBlock intBlock2 = block2.countBlock();
        return IntBlock.equals(intBlock1, intBlock2);
    }

    static int hash(AggregateMetricDoubleBlock block) {
        final int positions = block.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < positions; pos++) {
            if (block.isNull(pos)) {
                result = 31 * result - 1;
            } else {
                final int valueCount = block.getValueCount(pos);
                result = 31 * result + valueCount;
                final int firstValueIdx = block.getFirstValueIndex(pos);
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    for (DoubleBlock b : List.of(block.minBlock(), block.maxBlock(), block.sumBlock())) {
                        result *= 31;
                        result += b.isNull(firstValueIdx + valueIndex) ? -1 : Double.hashCode(b.getDouble(firstValueIdx + valueIndex));
                    }
                    result *= 31;
                    result += block.countBlock().isNull(firstValueIdx + valueIndex)
                        ? -1
                        : block.countBlock().getInt(firstValueIdx + valueIndex);
                }
            }
        }
        return result;
    }

    DoubleBlock minBlock();

    DoubleBlock maxBlock();

    DoubleBlock sumBlock();

    IntBlock countBlock();

    Block getMetricBlock(int index);
}
