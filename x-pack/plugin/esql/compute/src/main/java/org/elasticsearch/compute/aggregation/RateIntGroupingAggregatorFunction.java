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

import java.util.List;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link RateIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class RateIntGroupingAggregatorFunction implements GroupingAggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("rate", ElementType.BYTES_REF)
    );

    private final RateStates.GroupingState state;

    private final List<Integer> channels;

    private final DriverContext driverContext;

    private final BigArrays bigArrays;

    public RateIntGroupingAggregatorFunction(
        List<Integer> channels,
        RateStates.GroupingState state,
        DriverContext driverContext,
        BigArrays bigArrays
    ) {
        this.channels = channels;
        this.state = state;
        this.driverContext = driverContext;
        this.bigArrays = bigArrays;
    }

    public static RateIntGroupingAggregatorFunction create(List<Integer> channels, DriverContext driverContext, BigArrays bigArrays) {
        return new RateIntGroupingAggregatorFunction(channels, RateIntAggregator.initGrouping(bigArrays), driverContext, bigArrays);
    }

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public GroupingAggregatorFunction.AddInput prepareProcessPage(SeenGroupIds seenGroupIds, Page page) {
        Block uncastValuesBlock = page.getBlock(channels.get(0));
        if (uncastValuesBlock.areAllValuesNull()) {
            state.enableGroupIdTracking(seenGroupIds);
            return new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {}

                @Override
                public void add(int positionOffset, IntVector groupIds) {}
            };
        }

        IntBlock valuesBlock = (IntBlock) uncastValuesBlock;
        IntVector valuesVector = valuesBlock.asVector();
        LongBlock tsBlock = (LongVectorBlock) page.getBlock(1);  // FIXME
        LongVector timestamps = tsBlock.asVector();
        if (valuesVector == null) {
            if (valuesBlock.mayHaveNulls()) {
                state.enableGroupIdTracking(seenGroupIds);
            }
            return new GroupingAggregatorFunction.AddInput() {
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
        return new GroupingAggregatorFunction.AddInput() {
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

    private void addRawInput(int positionOffset, IntVector groups, IntBlock values, LongBlock timestamps) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int groupId = Math.toIntExact(groups.getInt(groupPosition));
            if (values.isNull(groupPosition + positionOffset)) {
                continue;
            }
            int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
            int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
            for (int v = valuesStart; v < valuesEnd; v++) {
                RateIntAggregator.combine(state, groupId, values.getInt(v), timestamps.getLong(v));
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, IntVector values, LongVector timestamps) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int groupId = Math.toIntExact(groups.getInt(groupPosition));
            int offset = groupPosition + positionOffset;
            RateIntAggregator.combine(state, groupId, values.getInt(offset), timestamps.getLong(offset));
        }
    }

    private void addRawInput(int positionOffset, IntBlock groups, IntBlock values, LongBlock timestamps) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = Math.toIntExact(groups.getInt(g));
                if (values.isNull(groupPosition + positionOffset)) {
                    continue;
                }
                int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
                int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
                for (int v = valuesStart; v < valuesEnd; v++) {
                    RateIntAggregator.combine(state, groupId, values.getInt(v), timestamps.getLong(v));
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntBlock groups, IntVector values, LongVector timestamps) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = Math.toIntExact(groups.getInt(g));
                int offset = groupPosition + positionOffset;
                RateIntAggregator.combine(state, groupId, values.getInt(offset), timestamps.getLong(offset));
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        assert channels.size() == intermediateBlockCount();
        BytesRefVector rate = page.<BytesRefBlock>getBlock(channels.get(0)).asVector();
        BytesRef scratch = new BytesRef();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int groupId = Math.toIntExact(groups.getInt(groupPosition));
            RateIntAggregator.combineIntermediate(state, groupId, rate.getBytesRef(groupPosition + positionOffset, scratch));
        }
    }

    @Override
    public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
        if (input.getClass() != getClass()) {
            throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
        }
        RateStates.GroupingState inState = ((RateIntGroupingAggregatorFunction) input).state;
        state.enableGroupIdTracking(new SeenGroupIds.Empty());
        RateIntAggregator.combineStates(state, groupId, inState, position);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        state.toIntermediate(blocks, offset, selected, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        BytesRefVector tsids = ((BytesRefBlock) blocks[1]).asVector();  // FIXME
        blocks[offset] = RateIntAggregator.evaluateFinal(state, selected, tsids, driverContext);
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
