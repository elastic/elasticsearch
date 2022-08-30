/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.aggregation;

import org.elasticsearch.xpack.sql.action.compute.data.AggregatorStateBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Block;
import org.elasticsearch.xpack.sql.action.compute.data.DoubleBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

// Max Aggregator of longs.
public class MaxAggregator implements AggregatorFunction {

    private final DoubleState state;
    private final int channel;

    static MaxAggregator create(int inputChannel) {
        if (inputChannel < 0) {
            throw new IllegalArgumentException();
        }
        return new MaxAggregator(inputChannel, new DoubleState());
    }

    static MaxAggregator createIntermediate() {
        return new MaxAggregator(-1, new DoubleState());
    }

    private MaxAggregator(int channel, DoubleState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Page page) {
        assert channel >= 0;
        Block block = page.getBlock(channel);
        DoubleState state = this.state;
        for (int i = 0; i < block.getPositionCount(); i++) {
            double next = block.getDouble(i);
            if (next > state.doubleValue()) {
                state.doubleValue(next);
            }
        }
    }

    @Override
    public void addIntermediateInput(Block block) {
        assert channel == -1;
        if (block instanceof AggregatorStateBlock) {
            @SuppressWarnings("unchecked")
            AggregatorStateBlock<DoubleState> blobBlock = (AggregatorStateBlock<DoubleState>) block;
            DoubleState state = this.state;
            DoubleState tmpState = new DoubleState();
            for (int i = 0; i < block.getPositionCount(); i++) {
                blobBlock.get(i, tmpState);
                state.doubleValue(Math.max(state.doubleValue(), tmpState.doubleValue()));
            }
        } else {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
    }

    @Override
    public Block evaluateIntermediate() {
        AggregatorStateBlock.Builder<AggregatorStateBlock<DoubleState>, DoubleState> builder = AggregatorStateBlock
            .builderOfAggregatorState(DoubleState.class);
        builder.add(state);
        return builder.build();
    }

    @Override
    public Block evaluateFinal() {
        return new DoubleBlock(new double[] { state.doubleValue() }, 1);
    }
}
