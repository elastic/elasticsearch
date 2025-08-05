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

public class SingletonLongsBuilder implements BlockLoader.SingletonLongBuilder, Releasable, Block.Builder {

    private final long[] values;
    private final BlockFactory blockFactory;

    private int count;

    public SingletonLongsBuilder(int initialSize, BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
        this.values = new long[initialSize];
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
        return (long) values.length * Long.BYTES;
    }

    @Override
    public Block build() {
        return blockFactory.newLongArrayVector(values, count, 0L).asBlock();
    }

    @Override
    public BlockLoader.SingletonLongBuilder appendLong(long value) {
        values[count++] = value;
        return this;
    }

    @Override
    public BlockLoader.SingletonLongBuilder appendLongs(long[] values, int from, int length) {
        System.arraycopy(values, from, this.values, count, length);
        count += length;
        return this;
    }

    @Override
    public void close() {

    }
}
