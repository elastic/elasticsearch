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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;

/**
 * A test BlockHash that wraps another one.
 * <p>
 *     Its methods can be overridden to implement custom behaviours or checks.
 * </p>
 */
public abstract class BlockHashWrapper extends BlockHash {
    protected BlockHash blockHash;

    public BlockHashWrapper(BlockFactory blockFactory, BlockHash blockHash) {
        super(blockFactory);
        this.blockHash = blockHash;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        blockHash.add(page, addInput);
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        return blockHash.lookup(page, targetBlockSize);
    }

    @Override
    public Block[] getKeys() {
        return blockHash.getKeys();
    }

    @Override
    public IntVector nonEmpty() {
        return blockHash.nonEmpty();
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return blockHash.seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        blockHash.close();
    }

    @Override
    public String toString() {
        return blockHash.toString();
    }
}
