/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;

/**
 * Maps a {@link BooleanBlock} column to group ids. Assigns group
 * {@code 0} to {@code false} and group {@code 1} to {@code true}.
 */
final class NullBlockHash extends BlockHash {
    private final int channel;
    private boolean seenNull = false;

    NullBlockHash(int channel, BlockFactory blockFactory) {
        super(blockFactory);
        this.channel = channel;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            seenNull = true;
            try (IntVector groupIds = blockFactory.newConstantIntVector(0, block.getPositionCount())) {
                addInput.add(0, groupIds);
            }
        } else {
            throw new IllegalArgumentException("can't use NullBlockHash for non-null blocks");
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        Block block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            return ReleasableIterator.single(blockFactory.newConstantIntVector(0, block.getPositionCount()).asBlock());
        }
        throw new IllegalArgumentException("can't use NullBlockHash for non-null blocks");
    }

    @Override
    public Block[] getKeys() {
        return new Block[] { blockFactory.newConstantNullBlock(seenNull ? 1 : 0) };
    }

    @Override
    public IntVector nonEmpty() {
        return blockFactory.newConstantIntVector(0, seenNull ? 1 : 0);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        BitArray seen = new BitArray(1, bigArrays);
        if (seenNull) {
            seen.set(0);
        }
        return seen;
    }

    @Override
    public void close() {
        // Nothing to close
    }

    @Override
    public String toString() {
        return "NullBlockHash{channel=" + channel + ", seenNull=" + seenNull + '}';
    }
}
