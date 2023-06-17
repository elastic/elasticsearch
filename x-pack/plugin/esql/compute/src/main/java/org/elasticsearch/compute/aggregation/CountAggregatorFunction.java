/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.List;

@Experimental
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
    public void addRawInput(Page page) {
        Block block = page.getBlock(channels.get(0));
        LongState state = this.state;
        state.longValue(state.longValue() + block.getTotalValueCount());
    }

    @Override
    public void addIntermediateInput(Page page) {
        Block block = page.getBlock(channels.get(0));
        if (block.asVector() != null && block.asVector() instanceof AggregatorStateVector) {
            @SuppressWarnings("unchecked")
            AggregatorStateVector<LongState> blobVector = (AggregatorStateVector) block.asVector();
            LongState state = this.state;
            LongState tmpState = new LongState();
            for (int i = 0; i < block.getPositionCount(); i++) {
                blobVector.get(i, tmpState);
                state.longValue(state.longValue() + tmpState.longValue());
            }
        } else {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset) {
        AggregatorStateVector.Builder<AggregatorStateVector<LongState>, LongState> builder = AggregatorStateVector.builderOfAggregatorState(
            LongState.class,
            state.getEstimatedSize()
        );
        builder.add(state, IntVector.range(0, 1));
        blocks[offset] = builder.build().asBlock();
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
