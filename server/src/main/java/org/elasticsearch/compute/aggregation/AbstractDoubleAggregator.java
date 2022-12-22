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
import org.elasticsearch.compute.data.Page;

@Experimental
abstract class AbstractDoubleAggregator implements AggregatorFunction {
    private final DoubleState state;
    private final int channel;

    protected AbstractDoubleAggregator(int channel, DoubleState state) {
        this.channel = channel;
        this.state = state;
    }

    protected abstract double combine(double current, double v);

    @Override
    public final void addRawInput(Page page) {
        assert channel >= 0;
        Block block = page.getBlock(channel);
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i) == false) {
                state.doubleValue(combine(state.doubleValue(), block.getDouble(i)));
            }
        }
    }

    @Override
    public final void addIntermediateInput(Block block) {
        assert channel == -1;
        if (false == block instanceof AggregatorStateBlock) {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
        @SuppressWarnings("unchecked")
        AggregatorStateBlock<DoubleState> blobBlock = (AggregatorStateBlock<DoubleState>) block;
        DoubleState tmpState = new DoubleState();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blobBlock.get(i, tmpState);
            state.doubleValue(combine(state.doubleValue(), tmpState.doubleValue()));
        }
    }

    @Override
    public final Block evaluateIntermediate() {
        AggregatorStateBlock.Builder<AggregatorStateBlock<DoubleState>, DoubleState> builder = AggregatorStateBlock
            .builderOfAggregatorState(DoubleState.class, state.getEstimatedSize());
        builder.add(state);
        return builder.build();
    }

    @Override
    public final Block evaluateFinal() {
        return new DoubleArrayBlock(new double[] { state.doubleValue() }, 1);
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channel=").append(channel);
        sb.append("]");
        return sb.toString();
    }
}
