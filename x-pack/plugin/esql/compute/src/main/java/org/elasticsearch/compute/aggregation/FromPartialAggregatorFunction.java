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
import org.elasticsearch.compute.data.IntVector;
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

    private final DriverContext driverContext;
    private final GroupingAggregatorFunction groupingAggregator;
    private final int inputChannel;
    private boolean receivedInput = false;

    public FromPartialAggregatorFunction(DriverContext driverContext, GroupingAggregatorFunction groupingAggregator, int inputChannel) {
        this.driverContext = driverContext;
        this.groupingAggregator = groupingAggregator;
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
        try (IntVector groupIds = driverContext.blockFactory().newConstantIntVector(0, page.getPositionCount())) {
            if (page.getPositionCount() > 0) {
                receivedInput = true;
            }
            final CompositeBlock inputBlock = page.getBlock(inputChannel);
            groupingAggregator.addIntermediateInput(0, groupIds, inputBlock.asPage());
        }
    }

    private IntVector outputPositions() {
        return driverContext.blockFactory().newConstantIntVector(0, receivedInput ? 1 : 0);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        final Block[] partialBlocks = new Block[groupingAggregator.intermediateBlockCount()];
        boolean success = false;
        try (IntVector selected = outputPositions()) {
            groupingAggregator.evaluateIntermediate(partialBlocks, 0, selected);
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
        try (IntVector selected = outputPositions()) {
            groupingAggregator.evaluateFinal(blocks, offset, selected, new GroupingAggregatorEvaluationContext(driverContext));
        }
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void close() {
        Releasables.close(groupingAggregator);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + "channel=" + inputChannel + ",delegate=" + groupingAggregator + "]";
    }
}
