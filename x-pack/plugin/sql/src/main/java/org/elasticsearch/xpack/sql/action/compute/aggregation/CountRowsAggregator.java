/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.aggregation;

import org.elasticsearch.xpack.sql.action.compute.data.AggregatorStateBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Block;
import org.elasticsearch.xpack.sql.action.compute.data.LongBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

public class CountRowsAggregator implements AggregatorFunction {

    private final LongState state;
    private final int channel;

    static CountRowsAggregator create(int inputChannel) {
        if (inputChannel < 0) {
            throw new IllegalArgumentException();
        }
        return new CountRowsAggregator(inputChannel, new LongState());
    }

    static CountRowsAggregator createIntermediate() {
        return new CountRowsAggregator(-1, new LongState());
    }

    private CountRowsAggregator(int channel, LongState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Page page) {
        assert channel >= 0;
        Block block = page.getBlock(channel);
        LongState state = this.state;
        state.longValue(state.longValue() + block.getPositionCount());
    }

    @Override
    public void addIntermediateInput(Block block) {
        assert channel == -1;
        if (block instanceof AggregatorStateBlock) {
            @SuppressWarnings("unchecked")
            AggregatorStateBlock<LongState> blobBlock = (AggregatorStateBlock<LongState>) block;
            LongState state = this.state;
            LongState tmpState = new LongState();
            for (int i = 0; i < block.getPositionCount(); i++) {
                blobBlock.get(i, tmpState);
                state.longValue(state.longValue() + tmpState.longValue());
            }
        } else {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
    }

    @Override
    public Block evaluateIntermediate() {
        AggregatorStateBlock.Builder<AggregatorStateBlock<LongState>, LongState> builder = AggregatorStateBlock.builderOfAggregatorState(
            LongState.class
        );
        builder.add(state);
        return builder.build();
    }

    @Override
    public Block evaluateFinal() {
        return new LongBlock(new long[] { state.longValue() }, 1);
    }
}
