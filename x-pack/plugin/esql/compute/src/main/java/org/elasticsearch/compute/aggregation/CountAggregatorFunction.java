/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;

import java.util.List;

public class CountAggregatorFunction implements AggregatorFunction {
    public static AggregatorFunctionSupplier supplier(BigArrays bigArrays, List<Integer> channels) {
        return new AggregatorFunctionSupplier() {
            @Override
            public AggregatorFunction aggregator() {
                return CountAggregatorFunction.create(channels);
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator() {
                return CountGroupingAggregatorFunction.create(bigArrays, channels);
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

    public static CountAggregatorFunction create(List<Integer> inputChannels) {
        return new CountAggregatorFunction(inputChannels, new LongState());
    }

    private CountAggregatorFunction(List<Integer> channels, LongState state) {
        this.channels = channels;
        this.state = state;
    }

    @Override
    public int intermediateBlockCount() {
        return intermediateStateDesc().size();
    }

    @Override
    public void addRawInput(Page page) {
        Block block = page.getBlock(channels.get(0));
        LongState state = this.state;
        state.longValue(state.longValue() + block.getTotalValueCount());
    }

    @Override
    public void addIntermediateInput(Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
        LongVector count = page.<LongBlock>getBlock(channels.get(0)).asVector();
        BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
        assert count.getPositionCount() == 1;
        assert count.getPositionCount() == seen.getPositionCount();
        state.longValue(state.longValue() + count.getLong(0));
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset) {
        state.toIntermediate(blocks, offset);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset) {
        blocks[offset] = LongBlock.newConstantBlockWith(state.longValue(), 1);
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
