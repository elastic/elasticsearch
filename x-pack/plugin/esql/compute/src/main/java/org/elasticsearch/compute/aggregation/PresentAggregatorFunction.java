/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

public class PresentAggregatorFunction implements AggregatorFunction {
    public static AggregatorFunctionSupplier supplier() {
        return new AggregatorFunctionSupplier() {
            @Override
            public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
                return PresentAggregatorFunction.intermediateStateDesc();
            }

            @Override
            public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
                return PresentGroupingAggregatorFunction.intermediateStateDesc();
            }

            @Override
            public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
                return PresentAggregatorFunction.create(channels);
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
                return PresentGroupingAggregatorFunction.create(driverContext, channels);
            }

            @Override
            public String describe() {
                return "present";
            }
        };
    }

    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("present", ElementType.BOOLEAN)
    );

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final List<Integer> channels;

    private boolean state;

    public static PresentAggregatorFunction create(List<Integer> inputChannels) {
        return new PresentAggregatorFunction(inputChannels, false);
    }

    private PresentAggregatorFunction(List<Integer> channels, boolean state) {
        this.channels = channels;
        this.state = state;
    }

    @Override
    public int intermediateBlockCount() {
        return intermediateStateDesc().size();
    }

    private int blockIndex() {
        return channels.get(0);
    }

    @Override
    public void addRawInput(Page page, BooleanVector mask) {
        if (mask.isConstant() && mask.getBoolean(0) == false) return;

        Block block = page.getBlock(blockIndex());
        this.state = mask.isConstant() ? block.getTotalValueCount() > 0 : presentMasked(block, mask);
    }

    private boolean presentMasked(Block block, BooleanVector mask) {
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (mask.getBoolean(p)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void addIntermediateInput(Page page) {
        assert channels.size() == intermediateBlockCount();
        var blockIndex = blockIndex();
        assert page.getBlockCount() >= blockIndex + intermediateStateDesc().size();
        Block uncastBlock = page.getBlock(channels.get(0));
        if (uncastBlock.areAllValuesNull()) {
            return;
        }
        BooleanVector present = page.<BooleanBlock>getBlock(channels.get(0)).asVector();
        assert present.getPositionCount() == 1;
        if (present.getBoolean(0)) {
            this.state = true;
        }
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        evaluateFinal(blocks, offset, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        blocks[offset] = driverContext.blockFactory().newConstantBooleanBlockWith(state, 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {}
}
