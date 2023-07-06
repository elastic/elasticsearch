/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

import java.util.List;

@Experimental
public class CountGroupingAggregatorFunction implements GroupingAggregatorFunction {

    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("count", ElementType.LONG),
        new IntermediateStateDesc("seen", ElementType.BOOLEAN)
    );

    private final LongArrayState state;
    private final List<Integer> channels;

    public static CountGroupingAggregatorFunction create(BigArrays bigArrays, List<Integer> inputChannels) {
        return new CountGroupingAggregatorFunction(inputChannels, new LongArrayState(bigArrays, 0));
    }

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private CountGroupingAggregatorFunction(List<Integer> channels, LongArrayState state) {
        this.channels = channels;
        this.state = state;
    }

    @Override
    public int intermediateBlockCount() {
        return intermediateStateDesc().size();
    }

    @Override
    public void addRawInput(LongVector groupIdVector, Page page) {
        Block valuesBlock = page.getBlock(channels.get(0));
        Vector valuesVector = valuesBlock.asVector();
        if (valuesVector == null) {
            addRawInput(groupIdVector, valuesBlock);
        } else {
            addRawInput(groupIdVector, valuesVector);
        }
    }

    @Override
    public void addRawInput(LongBlock groupIdBlock, Page page) {
        Block valuesBlock = page.getBlock(channels.get(0));
        Vector valuesVector = valuesBlock.asVector();
        if (valuesVector == null) {
            addRawInput(groupIdBlock, valuesBlock);
        } else {
            addRawInput(groupIdBlock, valuesVector);
        }
    }

    private void addRawInput(LongVector groups, Block values) {
        for (int position = 0; position < groups.getPositionCount(); position++) {
            int groupId = Math.toIntExact(groups.getLong(position));
            if (values.isNull(position)) {
                state.putNull(groupId);
                continue;
            }
            state.increment(values.getValueCount(position), groupId);
        }
    }

    private void addRawInput(LongVector groups, Vector values) {
        for (int position = 0; position < groups.getPositionCount(); position++) {
            int groupId = Math.toIntExact(groups.getLong(position));
            state.increment(1, groupId);
        }
    }

    private void addRawInput(LongBlock groups, Vector values) {
        for (int position = 0; position < groups.getPositionCount(); position++) {
            if (groups.isNull(position)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(position);
            int groupEnd = groupStart + groups.getValueCount(position);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = Math.toIntExact(groups.getLong(g));
                state.increment(1, groupId);
            }
        }
    }

    private void addRawInput(LongBlock groups, Block values) {
        if (values.areAllValuesNull()) {
            return;
        }
        for (int position = 0; position < groups.getPositionCount(); position++) {
            if (groups.isNull(position)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(position);
            int groupEnd = groupStart + groups.getValueCount(position);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = Math.toIntExact(groups.getLong(g));
                if (values.isNull(position)) {
                    state.putNull(groupId);
                    continue;
                }
                state.increment(values.getValueCount(position), groupId);
            }
        }
    }

    @Override
    public void addIntermediateInput(LongVector groupIdVector, Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
        LongVector count = page.<LongBlock>getBlock(channels.get(0)).asVector();
        BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
        assert count.getPositionCount() == seen.getPositionCount();
        for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
            state.increment(count.getLong(position), Math.toIntExact(groupIdVector.getLong(position)));
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
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        state.toIntermediate(blocks, offset, selected);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected) {
        LongVector.Builder builder = LongVector.newVectorBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            builder.appendLong(state.get(selected.getInt(i)));
        }
        blocks[offset] = builder.build().asBlock();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        state.close();
    }
}
