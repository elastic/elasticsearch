/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.index.mapper.BlockLoader;

public class AggregateDoubleMetricBlockBuilder extends AbstractBlockBuilder implements BlockLoader.AggregateDoubleMetricBuilder {

    private final DoubleBlockBuilder minBuilder;
    private final DoubleBlockBuilder maxBuilder;
    private final DoubleBlockBuilder sumBuilder;
    private final IntBlockBuilder countBuilder;

    public AggregateDoubleMetricBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        minBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
        maxBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
        sumBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
        countBuilder = new IntBlockBuilder(estimatedSize, blockFactory);
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
        minBuilder.copyFrom(composite.getBlock(Metric.MIN.ordinal()), beginInclusive, endExclusive);
        maxBuilder.copyFrom(composite.getBlock(Metric.MAX.ordinal()), beginInclusive, endExclusive);
        sumBuilder.copyFrom(composite.getBlock(Metric.SUM.ordinal()), beginInclusive, endExclusive);
        countBuilder.copyFrom(composite.getBlock(Metric.COUNT.ordinal()), beginInclusive, endExclusive);
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
        blocks[Metric.MIN.ordinal()] = minBuilder.build();
        blocks[Metric.MAX.ordinal()] = maxBuilder.build();
        blocks[Metric.SUM.ordinal()] = sumBuilder.build();
        blocks[Metric.COUNT.ordinal()] = countBuilder.build();
        return new CompositeBlock(blocks);
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
        MIN,
        MAX,
        SUM,
        COUNT;
    }
}
