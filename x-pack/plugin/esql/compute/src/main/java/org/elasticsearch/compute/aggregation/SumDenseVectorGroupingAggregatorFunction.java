/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

/**
 * Grouping aggregator function for summing dense vectors.
 * Each group computes the element-wise sum of all dense vectors in that group.
 * All vectors must have the same dimensions.
 */
public class SumDenseVectorGroupingAggregatorFunction implements GroupingAggregatorFunction {

    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(new IntermediateStateDesc("sum", ElementType.FLOAT));

    private final SumDenseVectorGroupingState state;
    private final List<Integer> channels;
    private final DriverContext driverContext;

    public SumDenseVectorGroupingAggregatorFunction(
        List<Integer> channels,
        SumDenseVectorGroupingState state,
        DriverContext driverContext
    ) {
        this.channels = channels;
        this.state = state;
        this.driverContext = driverContext;
    }

    public static SumDenseVectorGroupingAggregatorFunction create(List<Integer> channels, DriverContext driverContext) {
        return new SumDenseVectorGroupingAggregatorFunction(
            channels,
            new SumDenseVectorGroupingState(driverContext.bigArrays()),
            driverContext
        );
    }

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        FloatBlock valuesBlock = page.getBlock(channels.get(0));
        state.enableGroupIdTracking(seenGroupIds);
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

    private void addRawInput(int positionOffset, IntVector groups, FloatBlock valuesBlock) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuesPosition = groupPosition + positionOffset;
            if (valuesBlock.isNull(valuesPosition)) {
                continue;
            }
            int groupId = groups.getInt(groupPosition);
            float[] vector = extractVector(valuesBlock, valuesPosition);
            state.add(groupId, vector);
        }
    }

    private void addRawInput(int positionOffset, IntArrayBlock groups, FloatBlock valuesBlock) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            if (valuesBlock.isNull(valuesPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            float[] vector = extractVector(valuesBlock, valuesPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                state.add(groupId, vector);
            }
        }
    }

    private void addRawInput(int positionOffset, IntBigArrayBlock groups, FloatBlock valuesBlock) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            if (valuesBlock.isNull(valuesPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            float[] vector = extractVector(valuesBlock, valuesPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                state.add(groupId, vector);
            }
        }
    }

    private float[] extractVector(FloatBlock block, int position) {
        int valueCount = block.getValueCount(position);
        float[] vector = new float[valueCount];
        int start = block.getFirstValueIndex(position);
        for (int j = 0; j < valueCount; j++) {
            vector[j] = block.getFloat(start + j);
        }
        return vector;
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        FloatBlock sumBlock = page.getBlock(channels.get(0));
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            if (sumBlock.isNull(valuesPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            float[] vector = extractVector(sumBlock, valuesPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                state.add(groupId, vector);
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        FloatBlock sumBlock = page.getBlock(channels.get(0));
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            if (sumBlock.isNull(valuesPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            float[] vector = extractVector(sumBlock, valuesPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                state.add(groupId, vector);
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        FloatBlock sumBlock = page.getBlock(channels.get(0));
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuesPosition = groupPosition + positionOffset;
            if (sumBlock.isNull(valuesPosition)) {
                continue;
            }
            int groupId = groups.getInt(groupPosition);
            float[] vector = extractVector(sumBlock, valuesPosition);
            state.add(groupId, vector);
        }
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        state.enableGroupIdTracking(seenGroupIds);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        state.toIntermediate(blocks, offset, selected, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        blocks[offset] = state.toValuesBlock(selected, ctx.driverContext());
    }

    @Override
    public String toString() {
        return "SumDenseVectorGroupingAggregatorFunction[channels=" + channels + "]";
    }

    @Override
    public void close() {
        state.close();
    }
}
