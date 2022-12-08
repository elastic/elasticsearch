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
final class MinAggregator implements AggregatorFunction {

    private final DoubleState state;
    private final int channel;

    static MinAggregator create(int inputChannel) {
        return new MinAggregator(inputChannel, new DoubleState(Double.POSITIVE_INFINITY));
    }

    private MinAggregator(int channel, DoubleState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Page page) {
        assert channel >= 0;
        Block block = page.getBlock(channel);
        double min;
        if (block instanceof LongArrayBlock longBlock) {
            min = minFromLongBlock(longBlock);
        } else {
            min = minFromBlock(block);
        }
        state.doubleValue(Math.min(state.doubleValue(), min));
    }

    static double minFromBlock(Block block) {
        double min = Double.POSITIVE_INFINITY;
        int len = block.getPositionCount();
        if (block.areAllValuesNull() == false) {
            for (int i = 0; i < len; i++) {
                if (block.isNull(i) == false) {
                    min = Math.min(min, block.getDouble(i));
                }
            }
        }
        return min;
    }

    static double minFromLongBlock(LongArrayBlock block) {
        double min = Double.POSITIVE_INFINITY;
        if (block.areAllValuesNull() == false) {
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i) == false) {
                    min = Math.min(min, block.getLong(i));
                }
            }
        }
        return min;
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
                state.doubleValue(Math.min(state.doubleValue(), tmpState.doubleValue()));
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
