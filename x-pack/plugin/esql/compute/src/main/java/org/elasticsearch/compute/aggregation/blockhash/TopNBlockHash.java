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

// TODO: Do we need this class?
/**
 * {@link BlockHash} specialization for hashes that store only the top N elements.
 */
public abstract class TopNBlockHash extends BlockHash {

    TopNBlockHash(BlockFactory blockFactory) {
        super(blockFactory);
    }

    // TODO: Insert only if the value is within the top N
    @Override
    public abstract void add(Page page, GroupingAggregatorFunction.AddInput addInput);

    // TODO: Something to do with this?
    @Override
    public abstract ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize);

    // TODO: What happens with inserted but then ignored groups? (e.g. Top 2 DESC; 5, 4, 6 -> [6, 5], 4 discarded)
    @Override
    public abstract Block[] getKeys();

    // TODO: What to do with nonEmpty?
    @Override
    public abstract IntVector nonEmpty();

    // TODO: What to do with seenGroupIds?
    @Override
    public abstract BitArray seenGroupIds(BigArrays bigArrays);

    // TODO: Implement build() methods like in BlockHash
}
