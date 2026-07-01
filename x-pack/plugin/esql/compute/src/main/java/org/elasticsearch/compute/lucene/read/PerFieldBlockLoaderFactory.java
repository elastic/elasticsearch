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
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

public final class PerFieldBlockLoaderFactory implements BlockLoader.BlockFactory, Releasable {
    final BlockFactory factory;
    final NullBlockPool nullBlockPool;
    private final CircuitBreaker breaker;

    private long[] longPool;
    private int[] intPool;
    private double[] doublePool;

    private long usedLongBytes;
    private long usedIntBytes;
    private long usedDoubleBytes;

    public PerFieldBlockLoaderFactory(BlockFactory factory, NullBlockPool nullBlockPool) {
        this.factory = factory;
        this.nullBlockPool = nullBlockPool;
        this.breaker = factory.breaker();
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
    public BlockLoader.Block constantNulls(int count) {
        return nullBlockPool.constantNulls(count);
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
    public BlockLoader.Block constantLong(long value, int count) {
        return factory.newConstantLongBlockWith(value, count);
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
        return new SingletonLongBuilder(expectedCount, this);
    }

    @Override
    public BlockLoader.SingletonIntBuilder singletonInts(int expectedCount) {
        return new SingletonIntBuilder(expectedCount, this);
    }

    @Override
    public BlockLoader.SingletonDoubleBuilder singletonDoubles(int expectedCount) {
        return new SingletonDoubleBuilder(expectedCount, this);
    }

    long[] getLongs(int size) {
        long[] arr = longPool;
        longPool = null;
        if (arr == null || arr.length < size) {
            long newBytes = arrayBytes(Long.BYTES, size);
            if (newBytes > usedLongBytes) {
                breaker.addEstimateBytesAndMaybeBreak(newBytes - usedLongBytes, "long[]");
                usedLongBytes = newBytes;
            }
            arr = new long[size];
        }
        return arr;
    }

    void returnLongs(long[] arr) {
        longPool = arr;
    }

    int[] getInts(int size) {
        int[] arr = intPool;
        intPool = null;
        if (arr == null || arr.length < size) {
            long newBytes = arrayBytes(Integer.BYTES, size);
            if (newBytes > usedIntBytes) {
                breaker.addEstimateBytesAndMaybeBreak(newBytes - usedIntBytes, "int[]");
                usedIntBytes = newBytes;
            }
            arr = new int[size];
        }
        return arr;
    }

    void returnInts(int[] arr) {
        intPool = arr;
    }

    double[] getDoubles(int size) {
        double[] arr = doublePool;
        doublePool = null;
        if (arr == null || arr.length < size) {
            long newBytes = arrayBytes(Double.BYTES, size);
            if (newBytes > usedDoubleBytes) {
                breaker.addEstimateBytesAndMaybeBreak(newBytes - usedDoubleBytes, "double[]");
                usedDoubleBytes = newBytes;
            }
            arr = new double[size];
        }
        return arr;
    }

    void returnDoubles(double[] arr) {
        doublePool = arr;
    }

    private static long arrayBytes(int elementBytes, int size) {
        return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) elementBytes * size);
    }

    @Override
    public BlockLoader.Builder nulls(int expectedCount) {
        return ElementType.NULL.newBlockBuilder(expectedCount, factory);
    }

    @Override
    public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count, boolean isDense) {
        return new SingletonOrdinalsBuilder(this, ordinals, count, isDense);
    }

    @Override
    public BlockLoader.SortedSetOrdinalsBuilder sortedSetOrdinalsBuilder(SortedSetDocValues ordinals, int count) {
        return OrdinalsBuilder.sortedSet(factory, ordinals, count);
    }

    @Override
    public BlockLoader.SortedSetOrdinalsBuilder arrayOrderOrdinalsBuilder(SortedSetDocValues ordinals, int count) {
        return OrdinalsBuilder.arrayOrder(factory, ordinals, count);
    }

    @Override
    public BlockLoader.AggregateMetricDoubleBuilder aggregateMetricDoubleBuilder(int count) {
        return factory.newAggregateMetricDoubleBlockBuilder(count);
    }

    @Override
    public BlockLoader.LongRangeBuilder longRangeBuilder(int expectedCount) {
        return factory.newLongRangeBlockBuilder(expectedCount);
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
            (DoubleBlock) valueCounts,
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

    @Override
    public BlockLoader.TDigestBuilder tdigestBlockBuilder(int count) {
        return factory.newTDigestBlockBuilder(count);
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-(usedLongBytes + usedIntBytes + usedDoubleBytes));
    }

    public static class NullBlockPool implements Releasable {
        private final BlockFactory factory;
        private Block nullBlock;

        NullBlockPool(BlockFactory factory) {
            this.factory = factory;
        }

        public Block constantNulls(int count) {
            if (nullBlock == null) {
                nullBlock = factory.newConstantNullBlock(count);
            } else {
                if (nullBlock.getPositionCount() != count) {
                    nullBlock.close();
                    nullBlock = factory.newConstantNullBlock(count);
                }
            }
            nullBlock.incRef();
            return nullBlock;
        }

        @Override
        public void close() {
            if (nullBlock != null) {
                nullBlock.close();
            }
        }
    }
}
