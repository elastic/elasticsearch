/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.LongVectorBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link LastValueLongAggregator}.
 * This class is geneLastValued. Do not edit it.
 */
public final class LastValueLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("LastValue", ElementType.BYTES_REF)
    );

    private final LastValueStates.GroupingState state;

    private final List<Integer> channels;

    private final DriverContext driverContext;

    private final BigArrays bigArrays;

    public LastValueLongGroupingAggregatorFunction(
        List<Integer> channels,
        LastValueStates.GroupingState state,
        DriverContext driverContext,
        BigArrays bigArrays
    ) {
        this.channels = channels;
        this.state = state;
        this.driverContext = driverContext;
        this.bigArrays = bigArrays;
    }

    public static LastValueLongGroupingAggregatorFunction create(List<Integer> channels, DriverContext driverContext, BigArrays bigArrays) {
        return new LastValueLongGroupingAggregatorFunction(
            channels,
            LastValueLongAggregator.initGrouping(bigArrays),
            driverContext,
            bigArrays
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
    public AddInput prepareProcessPage(SeenGroupIds seenGroupIds, Page page) {
        Block uncastValuesBlock = page.getBlock(channels.get(0));
        if (uncastValuesBlock.areAllValuesNull()) {
            state.enableGroupIdTracking(seenGroupIds);
            return new AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {}

                @Override
                public void add(int positionOffset, IntVector groupIds) {}
            };
        }

        LongBlock valuesBlock = (LongBlock) uncastValuesBlock;
        LongVector valuesVector = valuesBlock.asVector();
        LongBlock tsBlock = (LongVectorBlock) page.getBlock(2);  // FIXME
        LongVector timestamps = tsBlock.asVector();
        if (valuesVector == null) {
            if (valuesBlock.mayHaveNulls()) {
                state.enableGroupIdTracking(seenGroupIds);
            }
            return new AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    addRawInput(positionOffset, groupIds, valuesBlock, tsBlock);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    addRawInput(positionOffset, groupIds, valuesBlock, tsBlock);
                }
            };
        }
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntBlock groupIds) {
                addRawInput(positionOffset, groupIds, valuesVector, timestamps);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addRawInput(positionOffset, groupIds, valuesVector, timestamps);
            }
        };
    }

    private void addRawInput(int positionOffset, IntVector groups, LongBlock values, LongBlock timestamps) {
        Set<Integer> seen = new HashSet<>();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int groupId = Math.toIntExact(groups.getInt(groupPosition));
            if (values.isNull(groupPosition + positionOffset) || seen.contains(groupId)) {
                continue;
            }
            seen.add(groupId);
            int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
            LastValueLongAggregator.combine(state, groupId, values.getLong(valuesStart), timestamps.getLong(valuesStart));
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, LongVector values, LongVector timestamps) {
        Set<Integer> seen = new HashSet<>();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int groupId = Math.toIntExact(groups.getInt(groupPosition));
            if (seen.contains(groupId)) {
                continue;
            }
            seen.add(groupId);
            int offset = groupPosition + positionOffset;
            LastValueLongAggregator.combine(state, groupId, values.getLong(offset), timestamps.getLong(offset));
        }
    }

    private void addRawInput(int positionOffset, IntBlock groups, LongBlock values, LongBlock timestamps) {
        Set<Integer> seen = new HashSet<>();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = Math.toIntExact(groups.getInt(g));
                if (values.isNull(groupPosition + positionOffset) || seen.contains(groupId)) {
                    continue;
                }
                seen.add(groupId);
                int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
                LastValueLongAggregator.combine(state, groupId, values.getLong(valuesStart), timestamps.getLong(valuesStart));
            }
        }
    }

    private void addRawInput(int positionOffset, IntBlock groups, LongVector values, LongVector timestamps) {
        Set<Integer> seen = new HashSet<>();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = Math.toIntExact(groups.getInt(g));
                if (seen.contains(groupId)) {
                    continue;
                }
                seen.add(groupId);
                int offset = groupPosition + positionOffset;
                LastValueLongAggregator.combine(state, groupId, values.getLong(offset), timestamps.getLong(offset));
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        assert channels.size() == intermediateBlockCount();
        BytesRefVector LastValue = page.<BytesRefBlock>getBlock(channels.get(0)).asVector();
        BytesRef scratch = new BytesRef();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int groupId = Math.toIntExact(groups.getInt(groupPosition));
            LastValueLongAggregator.combineIntermediate(state, groupId, LastValue.getBytesRef(groupPosition + positionOffset, scratch));
        }
    }

    @Override
    public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
        if (input.getClass() != getClass()) {
            throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
        }
        LastValueStates.GroupingState inState = ((LastValueLongGroupingAggregatorFunction) input).state;
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        LastValueLongAggregator.combineStates(state, groupId, inState, position);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        state.toIntermediate(blocks, offset, selected, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        blocks[offset] = LastValueLongAggregator.evaluateFinal(state, selected, driverContext);
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
