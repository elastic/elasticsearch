/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

import java.util.Optional;

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
        Optional<Vector> vector = block.asVector();
        if (vector.isPresent() == false || vector.get() instanceof AggregatorStateVector == false) {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
        @SuppressWarnings("unchecked")
        AggregatorStateVector<DoubleState> blobBlock = (AggregatorStateVector<DoubleState>) vector.get();
        DoubleState tmpState = new DoubleState();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blobBlock.get(i, tmpState);
            state.doubleValue(combine(state.doubleValue(), tmpState.doubleValue()));
        }
    }

    @Override
    public final Block evaluateIntermediate() {
        AggregatorStateVector.Builder<AggregatorStateVector<DoubleState>, DoubleState> builder = AggregatorStateVector
            .builderOfAggregatorState(DoubleState.class, state.getEstimatedSize());
        builder.add(state);
        return builder.build().asBlock();
    }

    @Override
    public final Block evaluateFinal() {
        return new DoubleVector(new double[] { state.doubleValue() }, 1).asBlock();
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
