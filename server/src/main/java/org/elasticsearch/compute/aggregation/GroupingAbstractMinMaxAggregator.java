/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.AggregatorStateBlock;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleArrayBlock;
import org.elasticsearch.compute.data.Page;

@Experimental
abstract class GroupingAbstractMinMaxAggregator implements GroupingAggregatorFunction {

    private final DoubleArrayState state;
    private final int channel;

    protected GroupingAbstractMinMaxAggregator(int channel, DoubleArrayState state) {
        this.channel = channel;
        this.state = state;
    }

    protected abstract double operator(double v1, double v2);

    protected abstract double initialDefaultValue();

    @Override
    public void addRawInput(Block groupIdBlock, Page page) {
        assert channel >= 0;
        Block valuesBlock = page.getBlock(channel);
        DoubleArrayState s = this.state;
        int len = valuesBlock.getPositionCount();
        for (int i = 0; i < len; i++) {
            int groupId = (int) groupIdBlock.getLong(i);
            s.set(operator(s.getOrDefault(groupId), valuesBlock.getDouble(i)), groupId);
        }
    }

    @Override
    public void addIntermediateInput(Block groupIdBlock, Block block) {
        assert channel == -1;
        if (block instanceof AggregatorStateBlock) {
            @SuppressWarnings("unchecked")
            AggregatorStateBlock<DoubleArrayState> blobBlock = (AggregatorStateBlock<DoubleArrayState>) block;
            // TODO exchange big arrays directly without funny serialization - no more copying
            DoubleArrayState tmpState = new DoubleArrayState(BigArrays.NON_RECYCLING_INSTANCE, initialDefaultValue());
            blobBlock.get(0, tmpState);
            final int positions = groupIdBlock.getPositionCount();
            final DoubleArrayState s = state;
            for (int i = 0; i < positions; i++) {
                int groupId = (int) groupIdBlock.getLong(i);
                s.set(operator(s.getOrDefault(groupId), tmpState.get(i)), groupId);
            }
        } else {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
    }

    @Override
    public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
        if (input.getClass() != getClass()) {
            throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
        }
        final DoubleArrayState inState = ((GroupingAbstractMinMaxAggregator) input).state;
        final double newValue = operator(state.getOrDefault(groupId), inState.get(position));
        state.set(newValue, groupId);
    }

    @Override
    public Block evaluateIntermediate() {
        AggregatorStateBlock.Builder<AggregatorStateBlock<DoubleArrayState>, DoubleArrayState> builder = AggregatorStateBlock
            .builderOfAggregatorState(DoubleArrayState.class, state.getEstimatedSize());
        builder.add(state);
        return builder.build();
    }

    @Override
    public Block evaluateFinal() {
        DoubleArrayState s = state;
        int positions = s.largestIndex + 1;
        double[] result = new double[positions];
        for (int i = 0; i < positions; i++) {
            result[i] = s.get(i);
        }
        return new DoubleArrayBlock(result, positions);
    }

    @Override
    public void close() {
        state.close();
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
