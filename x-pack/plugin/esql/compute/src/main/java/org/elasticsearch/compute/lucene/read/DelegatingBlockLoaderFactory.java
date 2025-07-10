/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.SortedDocValues;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.SingletonOrdinalsBuilder;
import org.elasticsearch.index.mapper.BlockLoader;

public abstract class DelegatingBlockLoaderFactory implements BlockLoader.BlockFactory {
    protected final BlockFactory factory;

    protected DelegatingBlockLoaderFactory(BlockFactory factory) {
        this.factory = factory;
    }

    @Override
    public BlockLoader.BooleanBuilder booleansFromDocValues(int expectedCount) {
        return factory.newBooleanBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
    }

    @Override
    public BlockLoader.BooleanBuilder booleans(int expectedCount) {
        return factory.newBooleanBlockBuilder(expectedCount);
    }

    @Override
    public BlockLoader.BytesRefBuilder bytesRefsFromDocValues(int expectedCount) {
        return factory.newBytesRefBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
    }

    @Override
    public BlockLoader.BytesRefBuilder bytesRefs(int expectedCount) {
        return factory.newBytesRefBlockBuilder(expectedCount);
    }

    @Override
    public BlockLoader.DoubleBuilder doublesFromDocValues(int expectedCount) {
        return factory.newDoubleBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
    }

    @Override
    public BlockLoader.DoubleBuilder doubles(int expectedCount) {
        return factory.newDoubleBlockBuilder(expectedCount);
    }

    @Override
    public BlockLoader.FloatBuilder denseVectors(int expectedVectorsCount, int dimensions) {
        return factory.newFloatBlockBuilder(expectedVectorsCount * dimensions);
    }

    @Override
    public BlockLoader.IntBuilder intsFromDocValues(int expectedCount) {
        return factory.newIntBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
    }

    @Override
    public BlockLoader.IntBuilder ints(int expectedCount) {
        return factory.newIntBlockBuilder(expectedCount);
    }

    @Override
    public BlockLoader.LongBuilder longsFromDocValues(int expectedCount) {
        return factory.newLongBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
    }

    @Override
    public BlockLoader.LongBuilder longs(int expectedCount) {
        return factory.newLongBlockBuilder(expectedCount);
    }

    @Override
    public BlockLoader.Builder nulls(int expectedCount) {
        return ElementType.NULL.newBlockBuilder(expectedCount, factory);
    }

    @Override
    public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count) {
        return new SingletonOrdinalsBuilder(factory, ordinals, count);
    }

    @Override
    public BlockLoader.AggregateMetricDoubleBuilder aggregateMetricDoubleBuilder(int count) {
        return factory.newAggregateMetricDoubleBlockBuilder(count);
    }
}
