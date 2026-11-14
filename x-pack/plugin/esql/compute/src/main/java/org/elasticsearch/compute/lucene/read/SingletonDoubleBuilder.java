/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.SingleArrayPool;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.util.BitSet;

/**
 * Like {@link org.elasticsearch.compute.data.DoubleBlockBuilder} but optimized for collecting single valued values, which may be null.
 * Additionally, this builder doesn't grow its array.
 */
public final class SingletonDoubleBuilder implements BlockLoader.SingletonDoubleBuilder, Releasable, Block.Builder {

    private final double[] values;
    private final int expectedCount;
    private final BlockFactory blockFactory;
    private final SingleArrayPool pool;

    private int count;
    private BitSet nulls;

    public SingletonDoubleBuilder(int expectedCount, BlockFactory blockFactory, @Nullable SingleArrayPool pool) {
        this.blockFactory = blockFactory;
        this.pool = pool;
        this.expectedCount = expectedCount;
        final double[] pooled = pool != null ? pool.getOrAllocateDoubles(expectedCount) : null;
        blockFactory.adjustBreaker(valuesSize(expectedCount));
        this.values = pooled != null ? pooled : new double[expectedCount];
    }

    @Override
    public Block.Builder appendNull() {
        if (nulls == null) {
            nulls = new BitSet();
        }
        nulls.set(count);
        values[count++] = 0;
        return this;
    }

    @Override
    public Block.Builder beginPositionEntry() {
        throw new UnsupportedOperationException();

    }

    @Override
    public Block.Builder endPositionEntry() {
        throw new UnsupportedOperationException();

    }

    @Override
    public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
        throw new UnsupportedOperationException();

    }

    @Override
    public Block.Builder mvOrdering(Block.MvOrdering mvOrdering) {
        throw new UnsupportedOperationException();

    }

    @Override
    public long estimatedBytes() {
        return valuesSize(expectedCount);
    }

    @Override
    public Block build() {
        if (expectedCount != count) {
            throw new IllegalStateException("expected " + expectedCount + " values but got " + count);
        }
        Block block = nulls == null
            ? blockFactory.newDoubleArrayVector(values, count).asBlock()
            : blockFactory.newDoubleArrayBlock(values, count, null, nulls, Block.MvOrdering.SORTED_ASCENDING);
        if (pool != null) {
            block.attachReleasable(() -> pool.putDoubles(values));
        }
        return block;
    }

    @Override
    public BlockLoader.SingletonDoubleBuilder appendDouble(double value) {
        values[count++] = value;
        return this;
    }

    @Override
    public BlockLoader.SingletonDoubleBuilder appendLongs(BlockDocValuesReader.ToDouble toDouble, long[] longValues, int from, int length) {
        for (int i = 0; i < length; i++) {
            values[count + i] = toDouble.convert(longValues[from + i]);
        }
        this.count += length;
        return this;
    }

    @Override
    public void close() {
        blockFactory.adjustBreaker(-valuesSize(expectedCount));
    }

    static long valuesSize(int count) {
        return (long) count * Double.BYTES;
    }
}
