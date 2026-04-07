/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

/**
 * Like {@link org.elasticsearch.compute.data.DoubleBlockBuilder} but optimized for collecting dense single valued values.
 * Additionally, this builder doesn't grow its array.
 */
public final class SingletonDoubleBuilder implements BlockLoader.SingletonDoubleBuilder, Releasable, Block.Builder {

    private final double[] values;
    private final BlockFactory blockFactory;

    private int count;

    public SingletonDoubleBuilder(int expectedCount, BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
        blockFactory.adjustBreaker(valuesSize(expectedCount));
        this.values = new double[expectedCount];
    }

    @Override
    public Block.Builder appendNull() {
        throw new UnsupportedOperationException();
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
        return valuesSize(values.length);
    }

    @Override
    public Block build() {
        if (values.length != count) {
            throw new IllegalStateException("expected " + values.length + " values but got " + count);
        }
        return blockFactory.newDoubleArrayVector(values, count).asBlock();
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
        blockFactory.adjustBreaker(-valuesSize(values.length));
    }

    static long valuesSize(int count) {
        return (long) count * Double.BYTES;
    }
}
