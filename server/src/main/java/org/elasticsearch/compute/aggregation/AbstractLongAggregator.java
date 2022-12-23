/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

import java.util.Optional;

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
        Optional<Vector> vector = block.asVector();
        if (vector.isPresent() == false || vector.get() instanceof AggregatorStateVector == false) {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
        @SuppressWarnings("unchecked")
        AggregatorStateVector<LongState> blobBlock = (AggregatorStateVector<LongState>) vector.get();
        LongState tmpState = new LongState();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blobBlock.get(i, tmpState);
            state.longValue(combine(state.longValue(), tmpState.longValue()));
        }
    }

    @Override
    public final Block evaluateIntermediate() {
        AggregatorStateVector.Builder<AggregatorStateVector<LongState>, LongState> builder = AggregatorStateVector.builderOfAggregatorState(
            LongState.class,
            state.getEstimatedSize()
        );
        builder.add(state);
        return builder.build().asBlock();
    }

    @Override
    public final Block evaluateFinal() {
        return new LongVector(new long[] { state.longValue() }, 1).asBlock();
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
