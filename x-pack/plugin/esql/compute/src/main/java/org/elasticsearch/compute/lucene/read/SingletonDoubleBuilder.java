/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

/**
 * Like {@link org.elasticsearch.compute.data.DoubleBlockBuilder} but optimized for collecting dense single valued values.
 * Additionally, this builder doesn't grow its array.
 */
public final class SingletonDoubleBuilder implements BlockLoader.SingletonDoubleBuilder, Releasable, Block.Builder {

    private final double[] values;
    private final int expectedCount;
    private final BlockFactory blockFactory;
    private boolean built;
    private int count;

    public SingletonDoubleBuilder(int expectedCount, BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
        this.expectedCount = expectedCount;
        this.values = blockFactory.pollDoubles(expectedCount);
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
        return valuesSize(expectedCount);
    }

    @Override
    public Block build() {
        if (expectedCount != count) {
            throw new IllegalStateException("expected " + expectedCount + " values but got " + count);
        }
        DoubleVector vector = blockFactory.newDoubleArrayVector(values, count, valuesSize(values.length));
        built = true;
        vector.pooled(true);
        return vector.asBlock();
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
        if (built == false) {
            built = true;
            blockFactory.recycleDoubles(values);
        }
    }

    static long valuesSize(int count) {
        return (long) count * Double.BYTES;
    }
}
