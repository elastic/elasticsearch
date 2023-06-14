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

@Experimental
public class CountAggregatorFunction implements AggregatorFunction {
    public static AggregatorFunctionSupplier supplier(BigArrays bigArrays, int channel) {
        return new AggregatorFunctionSupplier() {
            @Override
            public AggregatorFunction aggregator() {
                return CountAggregatorFunction.create(channel);
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator() {
                return CountGroupingAggregatorFunction.create(bigArrays, channel);
            }

            @Override
            public String describe() {
                return "count";
            }
        };
    }

    private final LongState state;
    private final int channel;

    public static CountAggregatorFunction create(int inputChannel) {
        return new CountAggregatorFunction(inputChannel, new LongState());
    }

    private CountAggregatorFunction(int channel, LongState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Page page) {
        Block block = page.getBlock(channel);
        LongState state = this.state;
        state.longValue(state.longValue() + block.getTotalValueCount());
    }

    @Override
    public void addIntermediateInput(Block block) {
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
    public Block evaluateIntermediate() {
        AggregatorStateVector.Builder<AggregatorStateVector<LongState>, LongState> builder = AggregatorStateVector.builderOfAggregatorState(
            LongState.class,
            state.getEstimatedSize()
        );
        builder.add(state, IntVector.range(0, 1));
        return builder.build().asBlock();
    }

    @Override
    public Block evaluateFinal() {
        return LongBlock.newConstantBlockWith(state.longValue(), 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channel=").append(channel);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        state.close();
    }
}
