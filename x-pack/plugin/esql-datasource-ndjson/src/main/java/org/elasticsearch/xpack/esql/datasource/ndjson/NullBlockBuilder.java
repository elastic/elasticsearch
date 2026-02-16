/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;

/**
 * Dummy builder for null blocks, for consistency with other types
 */
class NullBlockBuilder implements Block.Builder {
    int positions = 0;
    private Block block;
    private final BlockFactory blockFactory;

    NullBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
        this.blockFactory = blockFactory;
        this.block = blockFactory.newConstantNullBlock(estimatedSize);
    }

    @Override
    public Block.Builder appendNull() {
        positions++;
        return this;
    }

    @Override
    public Block.Builder beginPositionEntry() {
        positions++;
        return this;
    }

    @Override
    public Block.Builder endPositionEntry() {
        return this;
    }

    @Override
    public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
        positions += endExclusive - beginInclusive;
        return this;
    }

    @Override
    public Block.Builder mvOrdering(Block.MvOrdering mvOrdering) {
        return this;
    }

    @Override
    public long estimatedBytes() {
        return block.ramBytesUsed();
    }

    @Override
    public Block build() {
        if (positions == block.getPositionCount()) {
            return block;
        } else {
            block.close();
            return blockFactory.newConstantNullBlock(positions);
        }
    }

    @Override
    public void close() {
        block = null;
    }
}
