/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.AggregatorStateBlock;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.Page;

abstract class AbstractLongAggregator implements AggregatorFunction {
    private final LongState state;
    private final int channel;

    protected AbstractLongAggregator(int channel, LongState state) {
        this.channel = channel;
        this.state = state;
    }

    protected abstract long combine(long current, long v);

    @Override
    public final void addRawInput(Page page) {
        assert channel >= 0;
        Block block = page.getBlock(channel);
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i) == false) {
                state.longValue(combine(state.longValue(), block.getLong(i)));
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
        AggregatorStateBlock<LongState> blobBlock = (AggregatorStateBlock<LongState>) block;
        LongState tmpState = new LongState();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blobBlock.get(i, tmpState);
            state.longValue(combine(state.longValue(), tmpState.longValue()));
        }
    }

    @Override
    public final Block evaluateIntermediate() {
        AggregatorStateBlock.Builder<AggregatorStateBlock<LongState>, LongState> builder = AggregatorStateBlock.builderOfAggregatorState(
            LongState.class,
            state.getEstimatedSize()
        );
        builder.add(state);
        return builder.build();
    }

    @Override
    public final Block evaluateFinal() {
        return new LongArrayBlock(new long[] { state.longValue() }, 1);
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
