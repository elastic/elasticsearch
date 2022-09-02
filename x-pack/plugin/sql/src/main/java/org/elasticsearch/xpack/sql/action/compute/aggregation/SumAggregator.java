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
import org.elasticsearch.xpack.sql.action.compute.data.LongBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

// Sum Aggregator function.
public class SumAggregator implements AggregatorFunction {

    private final DoubleState state;
    private final int channel;

    static SumAggregator create(int inputChannel) {
        if (inputChannel < 0) {
            throw new IllegalArgumentException();
        }
        return new SumAggregator(inputChannel, new DoubleState());
    }

    static SumAggregator createIntermediate() {
        return new SumAggregator(-1, new DoubleState());
    }

    private SumAggregator(int channel, DoubleState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Page page) {
        assert channel >= 0;
        Block block = page.getBlock(channel);
        double sum;
        if (block instanceof LongBlock longBlock) {
            sum = sumFromLongBlock(longBlock);
        } else {
            sum = sumFromBlock(block);
        }
        state.doubleValue(state.doubleValue() + sum);
    }

    static double sumFromBlock(Block block) {
        double sum = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            sum += block.getDouble(i);
        }
        return sum;
    }

    static double sumFromLongBlock(LongBlock block) {
        double sum = 0;
        long[] values = block.getRawLongArray();
        for (int i = 0; i < block.getPositionCount(); i++) {
            sum += values[i];
        }
        return sum;
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
