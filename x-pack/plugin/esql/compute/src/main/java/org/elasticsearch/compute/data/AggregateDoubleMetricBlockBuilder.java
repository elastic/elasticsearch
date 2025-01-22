/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

public class AggregateDoubleMetricBlockBuilder extends AbstractBlockBuilder implements BlockLoader.AggregateDoubleMetricBuilder {

    private DoubleBlockBuilder minBuilder;
    private DoubleBlockBuilder maxBuilder;
    private DoubleBlockBuilder sumBuilder;
    private IntBlockBuilder countBuilder;

    public AggregateDoubleMetricBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
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
                Releasables.closeWhileHandlingException(minBuilder);
                Releasables.closeWhileHandlingException(maxBuilder);
                Releasables.closeWhileHandlingException(sumBuilder);
                Releasables.closeWhileHandlingException(countBuilder);
            }
        }
    }

    @Override
    protected int valuesLength() {
        return minBuilder.valuesLength();
    }

    @Override
    protected void growValuesArray(int newSize) {
        minBuilder.growValuesArray(newSize);
        maxBuilder.growValuesArray(newSize);
        sumBuilder.growValuesArray(newSize);
        countBuilder.growValuesArray(newSize);
    }

    @Override
    protected int elementSize() {
        return minBuilder.elementSize() + maxBuilder.elementSize() + sumBuilder.elementSize() + countBuilder.elementSize();
    }

    @Override
    public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
        CompositeBlock composite = (CompositeBlock) block;
        minBuilder.copyFrom(composite.getBlock(Metric.MIN.getIndex()), beginInclusive, endExclusive);
        maxBuilder.copyFrom(composite.getBlock(Metric.MAX.getIndex()), beginInclusive, endExclusive);
        sumBuilder.copyFrom(composite.getBlock(Metric.SUM.getIndex()), beginInclusive, endExclusive);
        countBuilder.copyFrom(composite.getBlock(Metric.COUNT.getIndex()), beginInclusive, endExclusive);
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
        try {
            finish();
            blocks[Metric.MIN.getIndex()] = minBuilder.build();
            blocks[Metric.MAX.getIndex()] = maxBuilder.build();
            blocks[Metric.SUM.getIndex()] = sumBuilder.build();
            blocks[Metric.COUNT.getIndex()] = countBuilder.build();
            return new CompositeBlock(blocks);
        } catch (CircuitBreakingException e) {
            for (Block block : blocks) {
                block.close();
            }
            throw e;
        }
    }

    @Override
    public BlockLoader.AggregateDoubleMetricBuilder append(double min, double max, double sum, int valueCount) {
        minBuilder.appendDouble(min);
        maxBuilder.appendDouble(max);
        sumBuilder.appendDouble(sum);
        countBuilder.appendInt(valueCount);
        return this;
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
}
