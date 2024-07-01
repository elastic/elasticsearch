/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * @see ToPartialGroupingAggregatorFunction
 */
public class ToPartialAggregatorFunction implements AggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("partial", ElementType.COMPOSITE, "partial_agg")
    );

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final AggregatorFunction delegate;
    private final List<Integer> channels;

    public ToPartialAggregatorFunction(AggregatorFunction delegate, List<Integer> channels) {
        this.delegate = delegate;
        this.channels = channels;
    }

    @Override
    public void addRawInput(Page page) {
        delegate.addRawInput(page);
    }

    @Override
    public void addIntermediateInput(Page page) {
        final CompositeBlock inputBlock = page.getBlock(channels.get(0));
        delegate.addIntermediateInput(inputBlock.asPage());
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        final Block[] partialBlocks = new Block[delegate.intermediateBlockCount()];
        boolean success = false;
        try {
            delegate.evaluateIntermediate(partialBlocks, 0, driverContext);
            blocks[offset] = new CompositeBlock(partialBlocks);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(partialBlocks);
            }
        }
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        evaluateIntermediate(blocks, offset, driverContext);
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void close() {
        Releasables.close(delegate);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + "channels=" + channels + ",delegate=" + delegate + "]";
    }
}
