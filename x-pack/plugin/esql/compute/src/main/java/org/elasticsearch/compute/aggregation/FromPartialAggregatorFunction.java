/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * @see ToPartialGroupingAggregatorFunction
 */
public class FromPartialAggregatorFunction implements AggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("partial", ElementType.COMPOSITE, "partial_agg")
    );

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final AggregatorFunction delegate;
    private final int inputChannel;

    public FromPartialAggregatorFunction(AggregatorFunction delegate, int inputChannel) {
        this.delegate = delegate;
        this.inputChannel = inputChannel;
    }

    @Override
    public void addRawInput(Page page, BooleanVector mask) {
        if (mask.isConstant() == false || mask.getBoolean(0) == false) {
            throw new IllegalStateException("can't mask partial");
        }
        addIntermediateInput(page);
    }

    @Override
    public void addIntermediateInput(Page page) {
        final CompositeBlock inputBlock = page.getBlock(inputChannel);
        final Page partials = inputBlock.asPage();
        for (int p = 0; p < partials.getPositionCount(); p++) {
            try (Page row = partials.slice(p, p + 1)) {
                delegate.addIntermediateInput(row);
            }
        }
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
        delegate.evaluateFinal(blocks, offset, driverContext);
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
        return getClass().getSimpleName() + "[" + "channel=" + inputChannel + ",delegate=" + delegate + "]";
    }
}
