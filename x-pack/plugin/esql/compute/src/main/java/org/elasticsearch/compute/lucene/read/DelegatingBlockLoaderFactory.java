/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

public abstract class DelegatingBlockLoaderFactory implements BlockLoader.BlockFactory {
    protected final BlockFactory factory;

    protected DelegatingBlockLoaderFactory(BlockFactory factory) {
        this.factory = factory;
    }

    @Override
    public void adjustBreaker(long delta) throws CircuitBreakingException {
        factory.adjustBreaker(delta);
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
    public BlockLoader.SingletonBytesRefBuilder singletonBytesRefs(int expectedCount) {
        return new SingletonBytesRefBuilder(expectedCount, factory);
    }

    @Override
    public BytesRefBlock constantBytes(BytesRef value, int count) {
        if (count == 1) {
            return factory.newConstantBytesRefBlockWith(value, count);
        }
        BytesRefVector dict = null;
        IntVector ordinals = null;
        boolean success = false;
        try {
            dict = factory.newConstantBytesRefVector(value, 1);
            ordinals = factory.newConstantIntVector(0, count);
            var result = new OrdinalBytesRefVector(ordinals, dict).asBlock();
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(dict, ordinals);
            }
        }
    }

    @Override
    public BlockLoader.Block constantInt(int value, int count) {
        return factory.newConstantIntVector(value, count).asBlock();
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
    public BlockLoader.SingletonLongBuilder singletonLongs(int expectedCount) {
        return new SingletonLongBuilder(expectedCount, factory);
    }

    @Override
    public BlockLoader.SingletonIntBuilder singletonInts(int expectedCount) {
        return new SingletonIntBuilder(expectedCount, factory);
    }

    @Override
    public BlockLoader.SingletonDoubleBuilder singletonDoubles(int expectedCount) {
        return new SingletonDoubleBuilder(expectedCount, factory);
    }

    @Override
    public BlockLoader.Builder nulls(int expectedCount) {
        return ElementType.NULL.newBlockBuilder(expectedCount, factory);
    }

    @Override
    public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count, boolean isDense) {
        return new SingletonOrdinalsBuilder(factory, ordinals, count, isDense);
    }

    @Override
    public BlockLoader.SortedSetOrdinalsBuilder sortedSetOrdinalsBuilder(SortedSetDocValues ordinals, int count) {
        return new SortedSetOrdinalsBuilder(factory, ordinals, count);
    }

    @Override
    public BlockLoader.AggregateMetricDoubleBuilder aggregateMetricDoubleBuilder(int count) {
        return factory.newAggregateMetricDoubleBlockBuilder(count);
    }

    @Override
    public BlockLoader.Block buildAggregateMetricDoubleDirect(
        BlockLoader.Block minBlock,
        BlockLoader.Block maxBlock,
        BlockLoader.Block sumBlock,
        BlockLoader.Block countBlock
    ) {
        return factory.newAggregateMetricDoubleBlockFromDocValues(
            (DoubleBlock) minBlock,
            (DoubleBlock) maxBlock,
            (DoubleBlock) sumBlock,
            (IntBlock) countBlock
        );
    }

    @Override
    public BlockLoader.ExponentialHistogramBuilder exponentialHistogramBlockBuilder(int count) {
        return factory.newExponentialHistogramBlockBuilder(count);
    }

    @Override
    public BlockLoader.Block buildExponentialHistogramBlockDirect(
        BlockLoader.Block minima,
        BlockLoader.Block maxima,
        BlockLoader.Block sums,
        BlockLoader.Block valueCounts,
        BlockLoader.Block zeroThresholds,
        BlockLoader.Block encodedHistograms
    ) {
        return factory.newExponentialHistogramBlockFromDocValues(
            (DoubleBlock) minima,
            (DoubleBlock) maxima,
            (DoubleBlock) sums,
            (LongBlock) valueCounts,
            (DoubleBlock) zeroThresholds,
            (BytesRefBlock) encodedHistograms
        );
    }

    @Override
    public BlockLoader.Block buildTDigestBlockDirect(
        BlockLoader.Block encodedDigests,
        BlockLoader.Block minima,
        BlockLoader.Block maxima,
        BlockLoader.Block sums,
        BlockLoader.Block valueCounts
    ) {
        return factory.newTDigestBlockFromDocValues(
            (BytesRefBlock) encodedDigests,
            (DoubleBlock) minima,
            (DoubleBlock) maxima,
            (DoubleBlock) sums,
            (LongBlock) valueCounts
        );
    }
}
