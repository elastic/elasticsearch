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
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

import java.util.Optional;

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
        final DoubleArrayState state = this.state;
        int len = valuesVector.getPositionCount();
        for (int i = 0; i < len; i++) {
            int groupId = Math.toIntExact(groupIdVector.getLong(i));
            state.set(operator(state.getOrDefault(groupId), valuesVector.getDouble(i)), groupId);
        }
    }

    private void addRawInputFromBlock(Vector groupIdVector, Block valuesBlock) {
        assert valuesBlock.elementType() == double.class;
        final DoubleArrayState state = this.state;
        int len = valuesBlock.getTotalValueCount();  // all values, for now
        for (int i = 0; i < len; i++) {
            int groupId = Math.toIntExact(groupIdVector.getLong(i));
            state.set(operator(state.getOrDefault(groupId), valuesBlock.getDouble(i)), groupId);
        }
    }

    @Override
    public void addIntermediateInput(Vector groupIdVector, Block block) {
        assert channel == -1;
        Optional<Vector> vector = block.asVector();
        if (vector.isPresent() && vector.get() instanceof AggregatorStateVector) {
            @SuppressWarnings("unchecked")
            AggregatorStateVector<DoubleArrayState> blobVector = (AggregatorStateVector<DoubleArrayState>) vector.get();
            // TODO exchange big arrays directly without funny serialization - no more copying
            DoubleArrayState tmpState = new DoubleArrayState(BigArrays.NON_RECYCLING_INSTANCE, initialDefaultValue());
            blobVector.get(0, tmpState);
            final int positions = groupIdVector.getPositionCount();
            final DoubleArrayState s = state;
            for (int i = 0; i < positions; i++) {
                int groupId = Math.toIntExact(groupIdVector.getLong(i));
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
        AggregatorStateVector.Builder<AggregatorStateVector<DoubleArrayState>, DoubleArrayState> builder = AggregatorStateVector
            .builderOfAggregatorState(DoubleArrayState.class, state.getEstimatedSize());
        builder.add(state);
        return builder.build().asBlock();
    }

    @Override
    public Block evaluateFinal() {
        DoubleArrayState s = state;
        int positions = s.largestIndex + 1;
        double[] values = new double[positions];
        for (int i = 0; i < positions; i++) {
            values[i] = s.get(i);
        }
        return new DoubleVector(values, positions).asBlock();
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
