/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

public class SparklineGroupingAggregatorFunction implements GroupingAggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("aggregator_intermediate_states", ElementType.COMPOSITE)
    );

    private final SparklineAggregator.GroupingState state;

    private final List<Integer> channels;

    private final DriverContext driverContext;

    public SparklineGroupingAggregatorFunction(
        List<Integer> channels,
        SparklineAggregator.GroupingState state,
        DriverContext driverContext
    ) {
        this.channels = channels;
        this.state = state;
        this.driverContext = driverContext;
    }

    public static SparklineGroupingAggregatorFunction create(
        List<Integer> channels,
        DriverContext driverContext,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate,
        AggregatorFunctionSupplier supplier
    ) {
        return new SparklineGroupingAggregatorFunction(
            channels,
            SparklineAggregator.initGrouping(driverContext.bigArrays(), dateBucketRounding, minDate, maxDate, supplier),
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
    public GroupingAggregatorFunction.AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        LongBlock trendValueBlock = page.getBlock(channels.get(0));
        LongBlock dateValueBlock = channels.size() == 2 ? page.getBlock(channels.get(1)) : page.getBlock(channels.get(0));
        LongVector trendValueVector = trendValueBlock.asVector();
        if (trendValueVector == null) {
            maybeEnableGroupIdTracking(seenGroupIds, trendValueBlock, dateValueBlock);
            return new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    addRawInput(positionOffset, groupIds, trendValueBlock, dateValueBlock);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    addRawInput(positionOffset, groupIds, trendValueBlock, dateValueBlock);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    addRawInput(positionOffset, groupIds, trendValueBlock, dateValueBlock);
                }

                @Override
                public void close() {}
            };
        }
        LongVector dateValueVector = dateValueBlock.asVector();
        if (dateValueVector == null) {
            maybeEnableGroupIdTracking(seenGroupIds, trendValueBlock, dateValueBlock);
            return new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    addRawInput(positionOffset, groupIds, trendValueBlock, dateValueBlock);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    addRawInput(positionOffset, groupIds, trendValueBlock, dateValueBlock);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    addRawInput(positionOffset, groupIds, trendValueBlock, dateValueBlock);
                }

                @Override
                public void close() {}
            };
        }
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, trendValueVector, dateValueVector);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, trendValueVector, dateValueVector);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addRawInput(positionOffset, groupIds, trendValueVector, dateValueVector);
            }

            @Override
            public void close() {}
        };
    }

    private void addRawInput(int positionOffset, IntArrayBlock groups, LongBlock trendValueBlock, LongBlock dateValueBlock) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            if (trendValueBlock.isNull(valuesPosition)) {
                continue;
            }
            if (dateValueBlock.isNull(valuesPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                int trendValueStart = trendValueBlock.getFirstValueIndex(valuesPosition);
                int trendValueEnd = trendValueStart + trendValueBlock.getValueCount(valuesPosition);
                for (int trendValueOffset = trendValueStart; trendValueOffset < trendValueEnd; trendValueOffset++) {
                    long trendValueValue = trendValueBlock.getLong(trendValueOffset);
                    int dateValueStart = dateValueBlock.getFirstValueIndex(valuesPosition);
                    int dateValueEnd = dateValueStart + dateValueBlock.getValueCount(valuesPosition);
                    for (int dateValueOffset = dateValueStart; dateValueOffset < dateValueEnd; dateValueOffset++) {
                        long dateValueValue = dateValueBlock.getLong(dateValueOffset);
                        SparklineAggregator.combine(driverContext, state, groupId, trendValueValue, dateValueValue);
                    }
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntArrayBlock groups, LongVector trendValueVector, LongVector dateValueVector) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                long trendValueValue = trendValueVector.getLong(valuesPosition);
                long dateValueValue = dateValueVector.getLong(valuesPosition);
                SparklineAggregator.combine(driverContext, state, groupId, trendValueValue, dateValueValue);
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        assert channels.size() == intermediateBlockCount();
        Block aggregatorIntermediateStatesUncast = page.getBlock(channels.get(0));
        if (aggregatorIntermediateStatesUncast.areAllValuesNull()) {
            return;
        }
        CompositeBlock compositeBlock = (CompositeBlock) aggregatorIntermediateStatesUncast;
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                int valuesPosition = groupPosition + positionOffset;
                SparklineAggregator.combineIntermediate(driverContext, state, groupId, compositeBlock, valuesPosition);
            }
        }
    }

    private void addRawInput(int positionOffset, IntBigArrayBlock groups, LongBlock trendValueBlock, LongBlock dateValueBlock) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            if (trendValueBlock.isNull(valuesPosition)) {
                continue;
            }
            if (dateValueBlock.isNull(valuesPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                int trendValueStart = trendValueBlock.getFirstValueIndex(valuesPosition);
                int trendValueEnd = trendValueStart + trendValueBlock.getValueCount(valuesPosition);
                for (int trendValueOffset = trendValueStart; trendValueOffset < trendValueEnd; trendValueOffset++) {
                    long trendValueValue = trendValueBlock.getLong(trendValueOffset);
                    int dateValueStart = dateValueBlock.getFirstValueIndex(valuesPosition);
                    int dateValueEnd = dateValueStart + dateValueBlock.getValueCount(valuesPosition);
                    for (int dateValueOffset = dateValueStart; dateValueOffset < dateValueEnd; dateValueOffset++) {
                        long dateValueValue = dateValueBlock.getLong(dateValueOffset);
                        SparklineAggregator.combine(driverContext, state, groupId, trendValueValue, dateValueValue);
                    }
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntBigArrayBlock groups, LongVector trendValueVector, LongVector dateValueVector) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                long trendValueValue = trendValueVector.getLong(valuesPosition);
                long dateValueValue = dateValueVector.getLong(valuesPosition);
                SparklineAggregator.combine(driverContext, state, groupId, trendValueValue, dateValueValue);
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        assert channels.size() == intermediateBlockCount();
        Block aggregatorIntermediateStatesUncast = page.getBlock(channels.get(0));
        if (aggregatorIntermediateStatesUncast.areAllValuesNull()) {
            return;
        }
        CompositeBlock compositeBlock = (CompositeBlock) aggregatorIntermediateStatesUncast;
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                int valuesPosition = groupPosition + positionOffset;
                SparklineAggregator.combineIntermediate(driverContext, state, groupId, compositeBlock, valuesPosition);
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, LongBlock trendValueBlock, LongBlock dateValueBlock) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuesPosition = groupPosition + positionOffset;
            if (trendValueBlock.isNull(valuesPosition)) {
                continue;
            }
            if (dateValueBlock.isNull(valuesPosition)) {
                continue;
            }
            int groupId = groups.getInt(groupPosition);
            int trendValueStart = trendValueBlock.getFirstValueIndex(valuesPosition);
            int trendValueEnd = trendValueStart + trendValueBlock.getValueCount(valuesPosition);
            for (int trendValueOffset = trendValueStart; trendValueOffset < trendValueEnd; trendValueOffset++) {
                long trendValueValue = trendValueBlock.getLong(trendValueOffset);
                int dateValueStart = dateValueBlock.getFirstValueIndex(valuesPosition);
                int dateValueEnd = dateValueStart + dateValueBlock.getValueCount(valuesPosition);
                for (int dateValueOffset = dateValueStart; dateValueOffset < dateValueEnd; dateValueOffset++) {
                    long dateValueValue = dateValueBlock.getLong(dateValueOffset);
                    SparklineAggregator.combine(driverContext, state, groupId, trendValueValue, dateValueValue);
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, LongVector trendValueVector, LongVector dateValueVector) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuesPosition = groupPosition + positionOffset;
            int groupId = groups.getInt(groupPosition);
            long trendValueValue = trendValueVector.getLong(valuesPosition);
            long dateValueValue = dateValueVector.getLong(valuesPosition);
            SparklineAggregator.combine(driverContext, state, groupId, trendValueValue, dateValueValue);
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        assert channels.size() == intermediateBlockCount();
        Block aggregatorIntermediateStatesUncast = page.getBlock(channels.get(0));
        if (aggregatorIntermediateStatesUncast.areAllValuesNull()) {
            return;
        }
        CompositeBlock compositeBlock = (CompositeBlock) aggregatorIntermediateStatesUncast;
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int groupId = groups.getInt(groupPosition);
            int valuesPosition = groupPosition + positionOffset;
            SparklineAggregator.combineIntermediate(driverContext, state, groupId, compositeBlock, valuesPosition);
        }
    }

    private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, LongBlock trendValueBlock, LongBlock dateValueBlock) {
        if (trendValueBlock.mayHaveNulls()) {
            state.enableGroupIdTracking(seenGroupIds);
        }
        if (dateValueBlock.mayHaveNulls()) {
            state.enableGroupIdTracking(seenGroupIds);
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
        blocks[offset] = SparklineAggregator.evaluateFinal(state, selected, ctx);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        state.close();
    }
}
