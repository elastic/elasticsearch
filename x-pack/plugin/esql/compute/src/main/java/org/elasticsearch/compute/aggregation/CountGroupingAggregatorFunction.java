/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

@Experimental
public class CountGroupingAggregatorFunction implements GroupingAggregatorFunction {

    private final LongArrayState state;
    private final int channel;

    static CountGroupingAggregatorFunction create(BigArrays bigArrays, int inputChannel) {
        return new CountGroupingAggregatorFunction(inputChannel, new LongArrayState(bigArrays, 0));
    }

    private CountGroupingAggregatorFunction(int channel, LongArrayState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(LongVector groupIdVector, Page page) {
        assert channel >= 0;
        assert groupIdVector.elementType() == ElementType.LONG;
        final Block valuesBlock = page.getBlock(channel);
        final Vector valuesVector = valuesBlock.asVector();
        if (valuesVector != null) {
            final int positions = groupIdVector.getPositionCount();
            for (int i = 0; i < positions; i++) {
                final int groupId = Math.toIntExact(groupIdVector.getLong(i));
                state.increment(1, groupId);
            }
        } else {
            // move the cold branch out of this method to keep the optimized case vector/vector as small as possible
            addRawInputWithBlockValues(groupIdVector, valuesBlock);
        }
    }

    @Override
    public void addRawInput(LongBlock groupIdBlock, Page page) {
        assert channel >= 0;
        assert groupIdBlock.elementType() == ElementType.LONG;
        final Block valuesBlock = page.getBlock(channel);
        final Vector valuesVector = valuesBlock.asVector();
        final int positions = groupIdBlock.getPositionCount();
        if (valuesVector != null) {
            for (int i = 0; i < positions; i++) {
                if (groupIdBlock.isNull(i) == false) {
                    final int groupId = Math.toIntExact(groupIdBlock.getLong(i));
                    state.increment(1, groupId);
                }
            }
        } else {
            for (int i = 0; i < positions; i++) {
                if (groupIdBlock.isNull(i) == false && valuesBlock.isNull(i) == false) {
                    final int groupId = Math.toIntExact(groupIdBlock.getLong(i));
                    state.increment(valuesBlock.getValueCount(i), groupId);  // counts values
                }
            }
        }
    }

    private void addRawInputWithBlockValues(LongVector groupIdVector, Block valuesBlock) {
        assert groupIdVector.elementType() == ElementType.LONG;
        final int positions = groupIdVector.getPositionCount();
        for (int i = 0; i < positions; i++) {
            if (valuesBlock.isNull(i) == false) {
                final int groupId = Math.toIntExact(groupIdVector.getLong(i));
                state.increment(valuesBlock.getValueCount(i), groupId);  // counts values
            }
        }
    }

    @Override
    public void addIntermediateInput(LongVector groupIdVector, Block block) {
        assert channel == -1;
        Vector vector = block.asVector();
        if (vector instanceof AggregatorStateVector) {
            @SuppressWarnings("unchecked")
            AggregatorStateVector<LongArrayState> blobBlock = (AggregatorStateVector<LongArrayState>) vector;
            // TODO exchange big arrays directly without funny serialization - no more copying
            LongArrayState tmpState = new LongArrayState(BigArrays.NON_RECYCLING_INSTANCE, 0);
            blobBlock.get(0, tmpState);
            final int positions = groupIdVector.getPositionCount();
            final LongArrayState state = this.state;
            for (int i = 0; i < positions; i++) {
                state.increment(tmpState.get(i), Math.toIntExact(groupIdVector.getLong(i)));
            }
        } else {
            throw new RuntimeException("expected AggregatorStateVector, got:" + block);
        }
    }

    @Override
    public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
        if (input.getClass() != getClass()) {
            throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
        }
        final LongArrayState inState = ((CountGroupingAggregatorFunction) input).state;
        state.increment(inState.get(position), groupId);
    }

    @Override
    public Block evaluateIntermediate(IntVector selected) {
        AggregatorStateVector.Builder<AggregatorStateVector<LongArrayState>, LongArrayState> builder = AggregatorStateVector
            .builderOfAggregatorState(LongArrayState.class, state.getEstimatedSize());
        builder.add(state, selected);
        return builder.build().asBlock();
    }

    @Override
    public Block evaluateFinal(IntVector selected) {
        return state.toValuesBlock(selected);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channel=").append(channel);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        state.close();
    }
}
