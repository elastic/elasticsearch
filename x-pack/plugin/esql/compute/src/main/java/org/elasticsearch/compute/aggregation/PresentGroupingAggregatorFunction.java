/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

public class PresentGroupingAggregatorFunction implements GroupingAggregatorFunction {

    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("present", ElementType.BOOLEAN),
        new IntermediateStateDesc("seen", ElementType.BOOLEAN)
    );

    private final BooleanArrayState state;
    private final List<Integer> channels;
    private final DriverContext driverContext;

    public static PresentGroupingAggregatorFunction create(DriverContext driverContext, List<Integer> inputChannels) {
        return new PresentGroupingAggregatorFunction(inputChannels, new BooleanArrayState(driverContext.bigArrays(), false), driverContext);
    }

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private PresentGroupingAggregatorFunction(List<Integer> channels, BooleanArrayState state, DriverContext driverContext) {
        this.channels = channels;
        this.state = state;
        this.driverContext = driverContext;
    }

    private int blockIndex() {
        return channels.get(0);
    }

    @Override
    public int intermediateBlockCount() {
        return intermediateStateDesc().size();
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        Block valuesBlock = page.getBlock(blockIndex());

        if (valuesBlock.mayHaveNulls()) {
            state.enableGroupIdTracking(seenGroupIds);
        }

        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addRawInput(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void close() {}
        };
    }

    private void addRawInput(int positionOffset, IntVector groups, Block values) {
        int position = positionOffset;
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++, position++) {
            if (values.isNull(position)) {
                continue;
            }
            int groupId = groups.getInt(groupPosition);
            state.set(groupId, state.getOrDefault(groupId) || values.getValueCount(position) > 0);
        }
    }

    private void addRawInput(int positionOffset, IntArrayBlock groups, Block values) {
        int position = positionOffset;
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++, position++) {
            if (groups.isNull(groupPosition) || values.isNull(position)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                state.set(groupId, state.getOrDefault(groupId) || values.getValueCount(position) > 0);
            }
        }
    }

    private void addRawInput(int positionOffset, IntBigArrayBlock groups, Block values) {
        int position = positionOffset;
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++, position++) {
            if (groups.isNull(groupPosition) || values.isNull(position)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                state.set(groupId, state.getOrDefault(groupId) || values.getValueCount(position) > 0);
            }
        }
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        state.enableGroupIdTracking(seenGroupIds);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= blockIndex() + intermediateStateDesc().size();
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        BooleanVector present = page.<BooleanBlock>getBlock(channels.get(0)).asVector();
        BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
        assert present.getPositionCount() == seen.getPositionCount();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                state.set(groupId, state.getOrDefault(groupId) || present.getBoolean(groupPosition + positionOffset));
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= blockIndex() + intermediateStateDesc().size();
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        BooleanVector present = page.<BooleanBlock>getBlock(channels.get(0)).asVector();
        BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
        assert present.getPositionCount() == seen.getPositionCount();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                state.set(groupId, state.getOrDefault(groupId) || present.getBoolean(groupPosition + positionOffset));
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= blockIndex() + intermediateStateDesc().size();
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        BooleanVector present = page.<BooleanBlock>getBlock(channels.get(0)).asVector();
        BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
        assert present.getPositionCount() == seen.getPositionCount();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int groupId = groups.getInt(groupPosition);
            state.set(groupId, state.getOrDefault(groupId) || present.getBoolean(groupPosition + positionOffset));
        }
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        state.toIntermediate(blocks, offset, selected, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evaluationContext) {
        try (BooleanVector.Builder builder = evaluationContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int si = selected.getInt(i);
                builder.appendBoolean(state.hasValue(si) && state.getOrDefault(si));
            }
            blocks[offset] = builder.build().asBlock();
        }
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
