/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

import java.util.Optional;

@Experimental
final class MaxAggregator implements AggregatorFunction {

    private final DoubleState state;
    private final int channel;

    static MaxAggregator create(int inputChannel) {
        return new MaxAggregator(inputChannel, new DoubleState(Double.NEGATIVE_INFINITY));
    }

    private MaxAggregator(int channel, DoubleState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Page page) {
        assert channel >= 0;
        Block block = page.getBlock(channel);
        double max;
        var vector = page.getBlock(channel).asVector();
        if (vector.isPresent()) {
            max = maxFromLongVector(vector.get());
        } else {
            max = maxFromBlock(block);
        }
        state.doubleValue(Math.max(state.doubleValue(), max));
    }

    private static double maxFromLongVector(Vector vector) {
        double max = Double.NEGATIVE_INFINITY;
        final int len = vector.getPositionCount();
        for (int i = 0; i < len; i++) {
            max = Math.max(max, vector.getLong(i));
        }
        return max;
    }

    private static double maxFromBlock(Block block) {
        double max = Double.NEGATIVE_INFINITY;
        int len = block.getPositionCount();
        if (block.areAllValuesNull() == false) {
            for (int i = 0; i < len; i++) {
                if (block.isNull(i) == false) {
                    max = Math.max(max, block.getDouble(i));
                }
            }
        }
        return max;
    }

    @Override
    public void addIntermediateInput(Block block) {
        assert channel == -1;
        Optional<Vector> vector = block.asVector();
        if (vector.isPresent() && vector.get() instanceof AggregatorStateVector) {
            @SuppressWarnings("unchecked")
            AggregatorStateVector<DoubleState> blobVector = (AggregatorStateVector<DoubleState>) vector.get();
            DoubleState state = this.state;
            DoubleState tmpState = new DoubleState();
            for (int i = 0; i < block.getPositionCount(); i++) {
                blobVector.get(i, tmpState);
                state.doubleValue(Math.max(state.doubleValue(), tmpState.doubleValue()));
            }
        } else {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
    }

    @Override
    public Block evaluateIntermediate() {
        AggregatorStateVector.Builder<AggregatorStateVector<DoubleState>, DoubleState> builder = AggregatorStateVector
            .builderOfAggregatorState(DoubleState.class, state.getEstimatedSize());
        builder.add(state);
        return builder.build().asBlock();
    }

    @Override
    public Block evaluateFinal() {
        return BlockBuilder.newConstantDoubleBlockWith(state.doubleValue(), 1);
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
