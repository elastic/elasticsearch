/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

import java.util.Optional;

@Experimental
public class GroupingCountAggregator implements GroupingAggregatorFunction {

    private final LongArrayState state;
    private final int channel;

    static GroupingCountAggregator create(BigArrays bigArrays, int inputChannel) {
        if (inputChannel < 0) {
            throw new IllegalArgumentException();
        }
        return new GroupingCountAggregator(inputChannel, new LongArrayState(bigArrays, 0));
    }

    static GroupingCountAggregator createIntermediate(BigArrays bigArrays) {
        return new GroupingCountAggregator(-1, new LongArrayState(bigArrays, 0));
    }

    private GroupingCountAggregator(int channel, LongArrayState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Vector groupIdVector, Page page) {
        assert channel >= 0;
        assert groupIdVector.elementType() == long.class;
        Block valuesBlock = page.getBlock(channel);
        Optional<Vector> vector = valuesBlock.asVector();
        if (vector.isPresent()) {
            addRawInputFromVector(groupIdVector, vector.get());
        } else {
            addRawInputFromBlock(groupIdVector, valuesBlock);
        }
    }

    private void addRawInputFromVector(Vector groupIdVector, Vector valuesVector) {
        final LongArrayState state = this.state;
        final int len = groupIdVector.getPositionCount();
        for (int i = 0; i < len; i++) {
            state.increment(1, Math.toIntExact(groupIdVector.getLong(i)));
        }
    }

    private void addRawInputFromBlock(Vector groupIdVector, Block valuesBlock) {
        final LongArrayState state = this.state;
        final int len = groupIdVector.getPositionCount();
        for (int i = 0; i < len; i++) {
            if (valuesBlock.isNull(i) == false) {
                state.increment(valuesBlock.getValueCount(i), Math.toIntExact(groupIdVector.getLong(i)));  // counts values
            }
        }
    }

    @Override
    public void addIntermediateInput(Vector groupIdVector, Block block) {
        assert channel == -1;
        Optional<Vector> vector = block.asVector();
        if (vector.isPresent() && vector.get() instanceof AggregatorStateVector) {
            @SuppressWarnings("unchecked")
            AggregatorStateVector<LongArrayState> blobBlock = (AggregatorStateVector<LongArrayState>) vector.get();
            // TODO exchange big arrays directly without funny serialization - no more copying
            LongArrayState tmpState = new LongArrayState(BigArrays.NON_RECYCLING_INSTANCE, 0);
            blobBlock.get(0, tmpState);
            final int positions = groupIdVector.getPositionCount();
            final LongArrayState state = this.state;
            for (int i = 0; i < positions; i++) {
                state.increment(tmpState.get(i), Math.toIntExact(groupIdVector.getLong(i)));
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
        final LongArrayState inState = ((GroupingCountAggregator) input).state;
        state.increment(inState.get(position), groupId);
    }

    @Override
    public Block evaluateIntermediate() {
        AggregatorStateVector.Builder<AggregatorStateVector<LongArrayState>, LongArrayState> builder = AggregatorStateVector
            .builderOfAggregatorState(LongArrayState.class, state.getEstimatedSize());
        builder.add(state);
        return builder.build().asBlock();
    }

    @Override
    public Block evaluateFinal() {
        LongArrayState s = state;
        int positions = s.largestIndex + 1;
        long[] result = new long[positions];
        for (int i = 0; i < positions; i++) {
            result[i] = s.get(i);
        }
        return new LongVector(result, positions).asBlock();
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
