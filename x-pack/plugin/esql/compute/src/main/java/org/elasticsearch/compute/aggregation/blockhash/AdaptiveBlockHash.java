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
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * A {@link BlockHash} that can adapt between multiple {@link BlockHash} implementations
 * based on the data being added and looked up. For example: if all input pages are non-null and single-valued (i.e., vectors),
 * then we can use a fast implementation without worrying about nulls or multi-valued entries. But if a page with nulls or
 * multi-valued entries is seen, we can switch to a more general implementation that handles those cases.
 */
abstract class AdaptiveBlockHash extends BlockHash {
    protected final List<GroupSpec> specs;
    protected final int emitBatchSize;

    protected BlockHash current;

    protected AdaptiveBlockHash(List<GroupSpec> specs, BlockFactory blockFactory, int emitBatchSize) {
        super(blockFactory);
        this.specs = specs;
        this.emitBatchSize = emitBatchSize;
    }

    @Override
    public final void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        prepareAddInput(page);
        current.add(page, addInput);
    }

    /**
     * Prepare the delegate block hash to add input from the given page.
     */
    protected abstract void prepareAddInput(Page page);

    @Override
    public final ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        prepareForLookup(page);
        return current.lookup(page, targetBlockSize);
    }

    /**
     * Prepare the delegate block hash for a lookup on the given page.
     */
    protected abstract void prepareForLookup(Page page);

    @Override
    public final Block[] getKeys() {
        return current.getKeys();
    }

    @Override
    public final IntVector nonEmpty() {
        return current.nonEmpty();
    }

    @Override
    public final int numKeys() {
        return current.numKeys();
    }

    @Override
    public final BitArray seenGroupIds(BigArrays bigArrays) {
        return current.seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        Releasables.close(current);
    }

    @Override
    public String toString() {
        return "Adaptive{" + current + "}";
    }
}
