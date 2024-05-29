/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

/**
 * Helper for adding a {@link Page} worth of {@link Block}s to a {@link BlockHash}.
 */
public class AbstractAddBlock implements Releasable {
    private final BlockFactory blockFactory;
    private final int emitBatchSize;
    private final GroupingAggregatorFunction.AddInput addInput;

    private int positionOffset = 0;
    private int added = 0;
    protected IntBlock.Builder ords;

    public AbstractAddBlock(BlockFactory blockFactory, int emitBatchSize, GroupingAggregatorFunction.AddInput addInput) {
        this.blockFactory = blockFactory;
        this.emitBatchSize = emitBatchSize;
        this.addInput = addInput;

        this.ords = blockFactory.newIntBlockBuilder(emitBatchSize);
    }

    protected final void addedValue(int position) {
        if (++added % emitBatchSize == 0) {
            rollover(position + 1);
        }
    }

    protected final void addedValueInMultivaluePosition(int position) {
        if (++added % emitBatchSize == 0) {
            ords.endPositionEntry();
            rollover(position);
            ords.beginPositionEntry();
        }
    }

    protected final void emitOrds() {
        try (IntBlock ordsBlock = ords.build()) {
            addInput.add(positionOffset, ordsBlock);
        }
    }

    private void rollover(int position) {
        emitOrds();
        positionOffset = position;
        ords = blockFactory.newIntBlockBuilder(emitBatchSize); // TODO add a clear method to the builder?
    }

    @Override
    public void close() {
        ords.close();
    }
}
