/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public final class AggregateMetricDoubleBlock extends AbstractNonThreadSafeRefCounted implements Block {
    private final DoubleBlock minBlock;
    private final DoubleBlock maxBlock;
    private final DoubleBlock sumBlock;
    private final IntBlock countBlock;
    private final int positionCount;

    public AggregateMetricDoubleBlock(DoubleBlock minBlock, DoubleBlock maxBlock, DoubleBlock sumBlock, IntBlock countBlock) {
        this.minBlock = minBlock;
        this.maxBlock = maxBlock;
        this.sumBlock = sumBlock;
        this.countBlock = countBlock;
        this.positionCount = minBlock.getPositionCount();
        for (Block b : List.of(minBlock, maxBlock, sumBlock, countBlock)) {
            if (b.getPositionCount() != positionCount) {
                assert false : "expected positionCount=" + positionCount + " but was " + b;
                throw new IllegalArgumentException("expected positionCount=" + positionCount + " but was " + b);
            }
            if (b.isReleased()) {
                assert false : "can't build aggregate_metric_double block out of released blocks but [" + b + "] was released";
                throw new IllegalArgumentException(
                    "can't build aggregate_metric_double block out of released blocks but [" + b + "] was released"
                );
            }
        }
    }

    public static AggregateMetricDoubleBlock fromCompositeBlock(CompositeBlock block) {
        assert block.getBlockCount() == 4
            : "Can't make AggregateMetricDoubleBlock out of CompositeBlock with " + block.getBlockCount() + " blocks";
        DoubleBlock min = block.getBlock(AggregateMetricDoubleBlockBuilder.Metric.MIN.getIndex());
        DoubleBlock max = block.getBlock(AggregateMetricDoubleBlockBuilder.Metric.MAX.getIndex());
        DoubleBlock sum = block.getBlock(AggregateMetricDoubleBlockBuilder.Metric.SUM.getIndex());
        IntBlock count = block.getBlock(AggregateMetricDoubleBlockBuilder.Metric.COUNT.getIndex());
        return new AggregateMetricDoubleBlock(min, max, sum, count);
    }

    public CompositeBlock asCompositeBlock() {
        final Block[] blocks = new Block[4];
        blocks[AggregateMetricDoubleBlockBuilder.Metric.MIN.getIndex()] = minBlock;
        blocks[AggregateMetricDoubleBlockBuilder.Metric.MAX.getIndex()] = maxBlock;
        blocks[AggregateMetricDoubleBlockBuilder.Metric.SUM.getIndex()] = sumBlock;
        blocks[AggregateMetricDoubleBlockBuilder.Metric.COUNT.getIndex()] = countBlock;
        return new CompositeBlock(blocks);
    }

    @Override
    protected void closeInternal() {
        Releasables.close(minBlock, maxBlock, sumBlock, countBlock);
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public int getTotalValueCount() {
        int totalValueCount = 0;
        for (Block b : List.of(minBlock, maxBlock, sumBlock, countBlock)) {
            totalValueCount += b.getTotalValueCount();
        }
        return totalValueCount;
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public int getFirstValueIndex(int position) {
        return minBlock.getFirstValueIndex(position);
    }

    @Override
    public int getValueCount(int position) {
        int max = 0;
        for (Block b : List.of(minBlock, maxBlock, sumBlock, countBlock)) {
            max = Math.max(max, b.getValueCount(position));
        }
        return max;
    }

    @Override
    public ElementType elementType() {
        return ElementType.AGGREGATE_METRIC_DOUBLE;
    }

    @Override
    public BlockFactory blockFactory() {
        return minBlock.blockFactory();
    }

    @Override
    public void allowPassingToDifferentDriver() {
        for (Block block : List.of(minBlock, maxBlock, sumBlock, countBlock)) {
            block.allowPassingToDifferentDriver();
        }
    }

    @Override
    public boolean isNull(int position) {
        for (Block block : List.of(minBlock, maxBlock, sumBlock, countBlock)) {
            if (block.isNull(position) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean mayHaveNulls() {
        return Stream.of(minBlock, maxBlock, sumBlock, countBlock).anyMatch(Block::mayHaveNulls);
    }

    @Override
    public boolean areAllValuesNull() {
        return Stream.of(minBlock, maxBlock, sumBlock, countBlock).allMatch(Block::areAllValuesNull);
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return Stream.of(minBlock, maxBlock, sumBlock, countBlock).anyMatch(Block::mayHaveMultivaluedFields);
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        if (Stream.of(minBlock, maxBlock, sumBlock, countBlock).noneMatch(Block::mayHaveMultivaluedFields)) {
            return false;
        }
        return Stream.of(minBlock, maxBlock, sumBlock, countBlock).anyMatch(Block::doesHaveMultivaluedFields);
    }

    @Override
    public Block filter(int... positions) {
        AggregateMetricDoubleBlock result = null;
        DoubleBlock newMinBlock = null;
        DoubleBlock newMaxBlock = null;
        DoubleBlock newSumBlock = null;
        IntBlock newCountBlock = null;
        try {
            newMinBlock = minBlock.filter(positions);
            newMaxBlock = maxBlock.filter(positions);
            newSumBlock = sumBlock.filter(positions);
            newCountBlock = countBlock.filter(positions);
            result = new AggregateMetricDoubleBlock(newMinBlock, newMaxBlock, newSumBlock, newCountBlock);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newMinBlock, newMaxBlock, newSumBlock, newCountBlock);
            }
        }
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        AggregateMetricDoubleBlock result = null;
        DoubleBlock newMinBlock = null;
        DoubleBlock newMaxBlock = null;
        DoubleBlock newSumBlock = null;
        IntBlock newCountBlock = null;
        try {
            newMinBlock = minBlock.keepMask(mask);
            newMaxBlock = maxBlock.keepMask(mask);
            newSumBlock = sumBlock.keepMask(mask);
            newCountBlock = countBlock.keepMask(mask);
            result = new AggregateMetricDoubleBlock(newMinBlock, newMaxBlock, newSumBlock, newCountBlock);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newMinBlock, newMaxBlock, newSumBlock, newCountBlock);
            }
        }
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        // TODO: support
        throw new UnsupportedOperationException("can't lookup values from AggregateMetricDoubleBlock");
    }

    @Override
    public MvOrdering mvOrdering() {
        // TODO: determine based on sub-blocks
        return MvOrdering.UNORDERED;
    }

    @Override
    public Block expand() {
        // TODO: support
        throw new UnsupportedOperationException("AggregateMetricDoubleBlock");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        for (Block block : List.of(minBlock, maxBlock, sumBlock, countBlock)) {
            block.writeTo(out);
        }
    }

    public static Block readFrom(StreamInput in) throws IOException {
        boolean success = false;
        DoubleBlock minBlock = null;
        DoubleBlock maxBlock = null;
        DoubleBlock sumBlock = null;
        IntBlock countBlock = null;
        BlockStreamInput blockStreamInput = (BlockStreamInput) in;
        try {
            minBlock = DoubleBlock.readFrom(blockStreamInput);
            maxBlock = DoubleBlock.readFrom(blockStreamInput);
            sumBlock = DoubleBlock.readFrom(blockStreamInput);
            countBlock = IntBlock.readFrom(blockStreamInput);
            AggregateMetricDoubleBlock result = new AggregateMetricDoubleBlock(minBlock, maxBlock, sumBlock, countBlock);
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.close(minBlock, maxBlock, sumBlock, countBlock);
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        return minBlock.ramBytesUsed() + maxBlock.ramBytesUsed() + sumBlock.ramBytesUsed() + countBlock.ramBytesUsed();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateMetricDoubleBlock that = (AggregateMetricDoubleBlock) o;
        return positionCount == that.positionCount
            && minBlock.equals(that.minBlock)
            && maxBlock.equals(that.maxBlock)
            && sumBlock.equals(that.sumBlock)
            && countBlock.equals(that.countBlock);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            DoubleBlock.hash(minBlock),
            DoubleBlock.hash(maxBlock),
            DoubleBlock.hash(sumBlock),
            IntBlock.hash(countBlock),
            positionCount
        );
    }

    public DoubleBlock minBlock() {
        return minBlock;
    }

    public DoubleBlock maxBlock() {
        return maxBlock;
    }

    public DoubleBlock sumBlock() {
        return sumBlock;
    }

    public IntBlock countBlock() {
        return countBlock;
    }

    public Block getMetricBlock(int index) {
        if (index == AggregateMetricDoubleBlockBuilder.Metric.MIN.getIndex()) {
            return minBlock;
        }
        if (index == AggregateMetricDoubleBlockBuilder.Metric.MAX.getIndex()) {
            return maxBlock;
        }
        if (index == AggregateMetricDoubleBlockBuilder.Metric.SUM.getIndex()) {
            return sumBlock;
        }
        if (index == AggregateMetricDoubleBlockBuilder.Metric.COUNT.getIndex()) {
            return countBlock;
        }
        throw new UnsupportedOperationException("Received an index (" + index + ") outside of range for AggregateMetricDoubleBlock.");
    }
}
