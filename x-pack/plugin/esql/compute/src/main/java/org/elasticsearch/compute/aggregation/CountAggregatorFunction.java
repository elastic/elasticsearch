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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

public class CountAggregatorFunction implements AggregatorFunction {
    public static AggregatorFunctionSupplier supplier(List<Integer> channels) {
        return new AggregatorFunctionSupplier() {
            @Override
            public AggregatorFunction aggregator(DriverContext driverContext) {
                return CountAggregatorFunction.create(channels);
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
                return CountGroupingAggregatorFunction.create(driverContext, channels);
            }

            @Override
            public String describe() {
                return "count";
            }
        };
    }

    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("count", ElementType.LONG),
        new IntermediateStateDesc("seen", ElementType.BOOLEAN)
    );

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final LongState state;
    private final List<Integer> channels;
    private final boolean countAll;

    public static CountAggregatorFunction create(List<Integer> inputChannels) {
        return new CountAggregatorFunction(inputChannels, new LongState());
    }

    private CountAggregatorFunction(List<Integer> channels, LongState state) {
        this.channels = channels;
        this.state = state;
        // no channels specified means count-all/count(*)
        this.countAll = channels.isEmpty();
    }

    @Override
    public int intermediateBlockCount() {
        return intermediateStateDesc().size();
    }

    private int blockIndex() {
        return countAll ? 0 : channels.get(0);
    }

    @Override
    public void addRawInput(Page page) {
        Block block = page.getBlock(blockIndex());
        LongState state = this.state;
        int count = countAll ? block.getPositionCount() : block.getTotalValueCount();
        state.longValue(state.longValue() + count);
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
        LongVector count = page.<LongBlock>getBlock(channels.get(0)).asVector();
        BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
        assert count.getPositionCount() == 1;
        assert count.getPositionCount() == seen.getPositionCount();
        state.longValue(state.longValue() + count.getLong(0));
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        state.toIntermediate(blocks, offset, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        blocks[offset] = driverContext.blockFactory().newConstantLongBlockWith(state.longValue(), 1);
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
    public void close() {
        state.close();
    }
}
