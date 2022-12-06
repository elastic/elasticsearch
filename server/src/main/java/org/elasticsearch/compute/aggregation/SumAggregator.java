/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.AggregatorStateBlock;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleArrayBlock;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.Page;

@Experimental
final class SumAggregator implements AggregatorFunction {

    private final DoubleState state;
    private final int channel;

    static SumAggregator create(int inputChannel) {
        return new SumAggregator(inputChannel, new DoubleState());
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
        if (block instanceof LongArrayBlock longBlock) {
            long cur = (long) state.doubleValue();
            state.doubleValue(Math.addExact(cur, sumFromLongBlock(longBlock)));
        } else {
            state.doubleValue(state.doubleValue() + sumFromBlock(block));
        }
    }

    static double sumFromBlock(Block block) {
        double sum = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            sum += block.getDouble(i);
        }
        return sum;
    }

    static long sumFromLongBlock(LongArrayBlock block) {
        long sum = 0;
        long[] values = block.getRawLongArray();
        for (int i = 0; i < values.length; i++) {
            try {
                sum = Math.addExact(sum, values[i]);
            } catch (ArithmeticException e) {
                var ex = new ArithmeticException("addition overflow"); // TODO: customize the exception
                ex.initCause(e);
                throw ex;
            }
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
                state.doubleValue(state.doubleValue() + tmpState.doubleValue());
            }
        } else {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
    }

    @Override
    public Block evaluateIntermediate() {
        AggregatorStateBlock.Builder<AggregatorStateBlock<DoubleState>, DoubleState> builder = AggregatorStateBlock
            .builderOfAggregatorState(DoubleState.class, state.getEstimatedSize());
        builder.add(state);
        return builder.build();
    }

    @Override
    public Block evaluateFinal() {
        return new DoubleArrayBlock(new double[] { state.doubleValue() }, 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channel=").append(channel);
        sb.append("]");
        return sb.toString();
    }
}
