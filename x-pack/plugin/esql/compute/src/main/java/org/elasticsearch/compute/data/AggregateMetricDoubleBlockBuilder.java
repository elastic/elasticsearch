/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

public class AggregateMetricDoubleBlockBuilder extends AbstractBlockBuilder implements BlockLoader.AggregateMetricDoubleBuilder {

    private DoubleBlockBuilder minBuilder;
    private DoubleBlockBuilder maxBuilder;
    private DoubleBlockBuilder sumBuilder;
    private IntBlockBuilder countBuilder;

    public AggregateMetricDoubleBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        minBuilder = null;
        maxBuilder = null;
        sumBuilder = null;
        countBuilder = null;
        try {
            minBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
            maxBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
            sumBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
            countBuilder = new IntBlockBuilder(estimatedSize, blockFactory);
        } finally {
            if (countBuilder == null) {
                Releasables.closeWhileHandlingException(minBuilder, maxBuilder, sumBuilder, countBuilder);
            }
        }
    }

    @Override
    protected int valuesLength() {
        throw new UnsupportedOperationException("Not available on aggregate_metric_double");
    }

    @Override
    protected void growValuesArray(int newSize) {
        throw new UnsupportedOperationException("Not available on aggregate_metric_double");
    }

    @Override
    protected int elementSize() {
        throw new UnsupportedOperationException("Not available on aggregate_metric_double");
    }

    @Override
    public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
        Block minBlock;
        Block maxBlock;
        Block sumBlock;
        Block countBlock;
        if (block.areAllValuesNull()) {
            minBlock = block;
            maxBlock = block;
            sumBlock = block;
            countBlock = block;
        } else {
            CompositeBlock composite = (CompositeBlock) block;
            minBlock = composite.getBlock(Metric.MIN.getIndex());
            maxBlock = composite.getBlock(Metric.MAX.getIndex());
            sumBlock = composite.getBlock(Metric.SUM.getIndex());
            countBlock = composite.getBlock(Metric.COUNT.getIndex());
        }
        minBuilder.copyFrom(minBlock, beginInclusive, endExclusive);
        maxBuilder.copyFrom(maxBlock, beginInclusive, endExclusive);
        sumBuilder.copyFrom(sumBlock, beginInclusive, endExclusive);
        countBuilder.copyFrom(countBlock, beginInclusive, endExclusive);
        return this;
    }

    @Override
    public AbstractBlockBuilder appendNull() {
        minBuilder.appendNull();
        maxBuilder.appendNull();
        sumBuilder.appendNull();
        countBuilder.appendNull();
        return this;
    }

    @Override
    public Block.Builder mvOrdering(Block.MvOrdering mvOrdering) {
        minBuilder.mvOrdering(mvOrdering);
        maxBuilder.mvOrdering(mvOrdering);
        sumBuilder.mvOrdering(mvOrdering);
        countBuilder.mvOrdering(mvOrdering);
        return this;
    }

    @Override
    public Block build() {
        Block[] blocks = new Block[4];
        boolean success = false;
        try {
            finish();
            blocks[Metric.MIN.getIndex()] = minBuilder.build();
            blocks[Metric.MAX.getIndex()] = maxBuilder.build();
            blocks[Metric.SUM.getIndex()] = sumBuilder.build();
            blocks[Metric.COUNT.getIndex()] = countBuilder.build();
            CompositeBlock block = new CompositeBlock(blocks);
            success = true;
            return block;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    protected void extraClose() {
        Releasables.closeExpectNoException(minBuilder, maxBuilder, sumBuilder, countBuilder);
    }

    @Override
    public BlockLoader.DoubleBuilder min() {
        return minBuilder;
    }

    @Override
    public BlockLoader.DoubleBuilder max() {
        return maxBuilder;
    }

    @Override
    public BlockLoader.DoubleBuilder sum() {
        return sumBuilder;
    }

    @Override
    public BlockLoader.IntBuilder count() {
        return countBuilder;
    }

    public enum Metric {
        MIN(0),
        MAX(1),
        SUM(2),
        COUNT(3);

        private final int index;

        Metric(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }

    public record AggregateMetricDoubleLiteral(Double min, Double max, Double sum, Integer count) {
        public AggregateMetricDoubleLiteral {
            min = min.isNaN() ? null : min;
            max = max.isNaN() ? null : max;
            sum = sum.isNaN() ? null : sum;
        }
    }
}
