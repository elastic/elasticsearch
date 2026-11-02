/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.List;
// end generated imports

/**
 * {@link AggregatorFunctionSupplier} implementation for PromQL changes over int values.
 * This class is generated. Edit {@code X-ChangesAggregatorFunctionSupplier.java.st} instead.
 */
public final class ChangesIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return ChangesIntGroupingAggregatorFunction.intermediateStateDesc();
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        return new ChangesIntGroupingAggregatorFunction(channels, driverContext);
    }

    @Override
    public String describe() {
        return "changes of ints";
    }
}

final class ChangesIntGroupingAggregatorFunction extends AbstractRateGroupingFunction implements GroupingAggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("timestamps", ElementType.LONG),
        new IntermediateStateDesc("values", ElementType.INT),
        new IntermediateStateDesc("sampleCounts", ElementType.LONG),
        new IntermediateStateDesc("changes", ElementType.LONG)
    );

    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    private final RateIntGroupingAggregatorFunction.IntRawBuffer rawBuffer;
    private final RateIntGroupingAggregatorFunction.IntervalBuffer intervalBuffer;
    private ObjectArray<ReducedState> reducedStates;
    private int lastSliceIndex = -1;

    ChangesIntGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.bigArrays = driverContext.bigArrays();
        RateIntGroupingAggregatorFunction.IntRawBuffer rawBuffer = null;
        RateIntGroupingAggregatorFunction.IntervalBuffer intervalBuffer = null;
        try {
            rawBuffer = new RateIntGroupingAggregatorFunction.IntRawBuffer(driverContext.breaker());
            intervalBuffer = new RateIntGroupingAggregatorFunction.IntervalBuffer(driverContext.breaker());
            this.reducedStates = bigArrays.newObjectArray(256);
            this.rawBuffer = rawBuffer;
            rawBuffer = null;
            this.intervalBuffer = intervalBuffer;
            intervalBuffer = null;
        } finally {
            Releasables.close(rawBuffer, intervalBuffer);
        }
    }

    static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public boolean canEmitPartialResultsPeriodically() {
        return false;
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        // Nulls are represented by missing reduced states.
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        IntBlock valuesBlock = page.getBlock(channels.get(0));
        if (valuesBlock.areAllValuesNull()) {
            return null;
        }
        LongVector timestampsVector = ((LongBlock) page.getBlock(channels.get(1))).asVector();
        if (timestampsVector == null) {
            assert false : "expected timestamp vector in time-series aggregation";
            throw new IllegalStateException("expected timestamp vector in time-series aggregation");
        }
        IntVector sliceIndices = ((IntBlock) page.getBlock(channels.get(2))).asVector();
        assert sliceIndices != null : "expected slice indices vector in time-series aggregation";
        int sliceIndex = sliceIndices.getInt(0);
        if (sliceIndex > lastSliceIndex) {
            flushRawBuffers();
            lastSliceIndex = sliceIndex;
        }
        IntVector valuesVector = valuesBlock.asVector();
        if (valuesVector != null) {
            return new AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groups) {
                    addRawInput(positionOffset, groups, valuesVector, timestampsVector);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groups) {
                    addRawInput(positionOffset, groups, valuesVector, timestampsVector);
                }

                @Override
                public void add(int positionOffset, IntVector groups) {
                    addRawInput(positionOffset, groups, valuesVector, timestampsVector);
                }

                @Override
                public void close() {}
            };
        }
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groups) {
                addRawInput(positionOffset, groups, valuesBlock, timestampsVector);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groups) {
                addRawInput(positionOffset, groups, valuesBlock, timestampsVector);
            }

            @Override
            public void add(int positionOffset, IntVector groups) {
                addRawInput(positionOffset, groups, valuesBlock, timestampsVector);
            }

            @Override
            public void close() {}
        };
    }

    private void addRawInput(int positionOffset, IntBlock groups, IntBlock values, LongVector timestamps) {
        for (int p = 0; p < groups.getPositionCount(); p++) {
            int valuePosition = positionOffset + p;
            if (groups.isNull(p) || values.isNull(valuePosition)) {
                continue;
            }
            assert values.getValueCount(valuePosition) == 1 : "expected single-valued block " + values;
            int value = values.getInt(values.getFirstValueIndex(valuePosition));
            long timestamp = timestamps.getLong(valuePosition);
            int groupStart = groups.getFirstValueIndex(p);
            int groupEnd = groupStart + groups.getValueCount(p);
            for (int g = groupStart; g < groupEnd; g++) {
                rawBuffer.append(groups.getInt(g), timestamp, value);
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, IntBlock values, LongVector timestamps) {
        for (int p = 0; p < groups.getPositionCount(); p++) {
            int valuePosition = positionOffset + p;
            if (values.isNull(valuePosition)) {
                continue;
            }
            assert values.getValueCount(valuePosition) == 1 : "expected single-valued block " + values;
            int value = values.getInt(values.getFirstValueIndex(valuePosition));
            rawBuffer.append(groups.getInt(p), timestamps.getLong(valuePosition), value);
        }
    }

    private void addRawInput(int positionOffset, IntBlock groups, IntVector values, LongVector timestamps) {
        for (int p = 0; p < groups.getPositionCount(); p++) {
            if (groups.isNull(p)) {
                continue;
            }
            int valuePosition = positionOffset + p;
            int value = values.getInt(valuePosition);
            long timestamp = timestamps.getLong(valuePosition);
            int groupStart = groups.getFirstValueIndex(p);
            int groupEnd = groupStart + groups.getValueCount(p);
            for (int g = groupStart; g < groupEnd; g++) {
                rawBuffer.append(groups.getInt(g), timestamp, value);
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, IntVector values, LongVector timestamps) {
        for (int p = 0; p < groups.getPositionCount(); p++) {
            int valuePosition = positionOffset + p;
            rawBuffer.append(groups.getInt(p), timestamps.getLong(valuePosition), values.getInt(valuePosition));
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        addIntermediateInputBlock(positionOffset, groups, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        addIntermediateInputBlock(positionOffset, groups, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        LongBlock timestamps = page.getBlock(channels.get(0));
        IntBlock values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        LongVector sampleCounts = ((LongBlock) page.getBlock(channels.get(2))).asVector();
        LongVector changes = ((LongBlock) page.getBlock(channels.get(3))).asVector();
        if (sampleCounts == null || changes == null) {
            return;
        }
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            long sampleCount = sampleCounts.getLong(valuePosition);
            if (sampleCount > 0) {
                addIntermediatePosition(
                    groups.getInt(groupPosition),
                    timestamps,
                    values,
                    sampleCount,
                    changes.getLong(valuePosition),
                    valuePosition
                );
            }
        }
    }

    private void addIntermediateInputBlock(int positionOffset, IntBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        LongBlock timestamps = page.getBlock(channels.get(0));
        IntBlock values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        LongVector sampleCounts = ((LongBlock) page.getBlock(channels.get(2))).asVector();
        LongVector changes = ((LongBlock) page.getBlock(channels.get(3))).asVector();
        if (sampleCounts == null || changes == null) {
            return;
        }
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            long sampleCount = sampleCounts.getLong(valuePosition);
            if (sampleCount == 0 || groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                addIntermediatePosition(groups.getInt(g), timestamps, values, sampleCount, changes.getLong(valuePosition), valuePosition);
            }
        }
    }

    private void addIntermediatePosition(int groupId, LongBlock timestamps, IntBlock values, long sampleCount, long changes, int position) {
        ReducedState state = getOrInitializeReducedState(groupId);
        state.appendIntervalsFromBlocks(timestamps, values, position);
        state.samples += sampleCount;
        state.changes += changes;
    }

    @Override
    public PreparedForEvaluation prepareEvaluateIntermediate(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        flushRawBuffers();
        return this::evaluateIntermediate;
    }

    private void evaluateIntermediate(Block[] blocks, int offset, IntVector selectedInPage) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selectedInPage.getPositionCount();
        try (
            LongBlock.Builder timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
            IntBlock.Builder values = blockFactory.newIntBlockBuilder(positionCount * 2);
            LongVector.FixedBuilder sampleCounts = blockFactory.newLongVectorFixedBuilder(positionCount);
            LongVector.FixedBuilder changes = blockFactory.newLongVectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                ReducedState state = group < reducedStates.size() ? reducedStates.get(group) : null;
                // Keep intervals separate until final evaluation, matching rate's reduced-state contract.
                if (state != null && state.samples > 0) {
                    state.writeIntervalsToBlocks(timestamps, values);
                    sampleCounts.appendLong(state.samples);
                    changes.appendLong(state.changes);
                } else {
                    timestamps.appendLong(0);
                    values.appendInt(0);
                    sampleCounts.appendLong(0);
                    changes.appendLong(0);
                }
            }
            blocks[offset] = timestamps.build();
            blocks[offset + 1] = values.build();
            blocks[offset + 2] = sampleCounts.build().asBlock();
            blocks[offset + 3] = changes.build().asBlock();
        }
    }

    @Override
    public PreparedForEvaluation prepareEvaluateFinal(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        flushRawBuffers();
        return this::evaluateFinal;
    }

    private void evaluateFinal(Block[] blocks, int offset, IntVector selectedInPage) {
        int positionCount = selectedInPage.getPositionCount();
        try (LongBlock.Builder changes = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                ReducedState state = group < reducedStates.size() ? reducedStates.get(group) : null;
                if (state == null || state.samples == 0) {
                    changes.appendNull();
                } else {
                    state.combineIntervals();
                    changes.appendLong(state.changes);
                }
            }
            blocks[offset] = changes.build();
        }
    }

    private ReducedState getOrInitializeReducedState(int groupId) {
        reducedStates = bigArrays.grow(reducedStates, groupId + 1);
        ReducedState state = reducedStates.get(groupId);
        if (state == null) {
            state = new ReducedState();
            reducedStates.set(groupId, state);
        }
        return state;
    }

    private void flushRawBuffers() {
        if (rawBuffer.minGroupId > rawBuffer.maxGroupId) {
            return;
        }
        reducedStates = bigArrays.grow(reducedStates, rawBuffer.maxGroupId + 1);
        FlushQueues flushQueues = rawBuffer.prepareForFlush();
        for (int groupId = flushQueues.minGroupId(); groupId <= flushQueues.maxGroupId(); groupId++) {
            FlushQueue flushQueue = flushQueues.getFlushQueue(groupId);
            if (flushQueue != null) {
                flushGroup(getOrInitializeReducedState(groupId), flushQueue);
            }
        }
        rawBuffer.clearBuffers();
    }

    private void flushGroup(ReducedState state, FlushQueue flushQueue) {
        int sampleCount = flushQueue.valueCount;
        int lastPosition = nextPosition(flushQueue);
        int previous = lastPosition;
        int firstPosition = lastPosition;
        while (flushQueue.size() > 0) {
            int current = nextPosition(flushQueue);
            if (valuesEqual(rawBuffer.value(previous), rawBuffer.value(current)) == false) {
                state.changes++;
            }
            previous = current;
            firstPosition = current;
        }
        state.samples += sampleCount;
        state.appendInterval(
            rawBuffer.timestamp(lastPosition),
            rawBuffer.value(lastPosition),
            rawBuffer.timestamp(firstPosition),
            rawBuffer.value(firstPosition)
        );
    }

    private static int nextPosition(FlushQueue flushQueue) {
        Slice top = flushQueue.top();
        int position = top.next();
        if (top.exhausted()) {
            flushQueue.pop();
        } else {
            flushQueue.updateTop();
        }
        return position;
    }

    private static boolean valuesEqual(int left, int right) {
        return left == right;
    }

    @Override
    public void close() {
        Releasables.close(reducedStates, rawBuffer, intervalBuffer);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    private final class ReducedState {
        private static final int[] EMPTY_INTERVALS = new int[0];
        private long samples;
        private long changes;
        private int[] intervals = EMPTY_INTERVALS;

        private void appendInterval(long lastTs, int lastValue, long firstTs, int firstValue) {
            int currentSize = intervals.length;
            intervals = ArrayUtil.growExact(intervals, currentSize + 1);
            intervals[currentSize] = intervalBuffer.appendInterval(lastTs, lastValue, firstTs, firstValue);
        }

        private void appendIntervalsFromBlocks(LongBlock timestamps, IntBlock values, int position) {
            int intervalCount = timestamps.getValueCount(position) / 2;
            int firstIntervalId = intervalBuffer.appendIntervalsFromBlocks(timestamps, values, position);
            int currentSize = intervals.length;
            intervals = ArrayUtil.growExact(intervals, currentSize + intervalCount);
            for (int i = 0; i < intervalCount; i++) {
                intervals[currentSize++] = firstIntervalId + i;
            }
        }

        private void writeIntervalsToBlocks(LongBlock.Builder timestamps, IntBlock.Builder values) {
            timestamps.beginPositionEntry();
            values.beginPositionEntry();
            for (int intervalId : intervals) {
                timestamps.appendLong(intervalBuffer.lastTs(intervalId));
                timestamps.appendLong(intervalBuffer.firstTs(intervalId));
                values.appendInt(intervalBuffer.lastValue(intervalId));
                values.appendInt(intervalBuffer.firstValue(intervalId));
            }
            timestamps.endPositionEntry();
            values.endPositionEntry();
        }

        private void combineIntervals() {
            sortIntervals();
            for (int i = 1; i < intervals.length; i++) {
                int next = intervals[i - 1];
                int previous = intervals[i];
                if (valuesEqual(intervalBuffer.lastValue(previous), intervalBuffer.firstValue(next)) == false) {
                    changes++;
                }
            }
        }

        private void sortIntervals() {
            new IntroSorter() {
                private long pivotTs;

                @Override
                protected void setPivot(int i) {
                    pivotTs = intervalBuffer.lastTs(intervals[i]);
                }

                @Override
                protected int comparePivot(int j) {
                    return Long.compare(intervalBuffer.lastTs(intervals[j]), pivotTs);
                }

                @Override
                protected int compare(int i, int j) {
                    return Long.compare(intervalBuffer.lastTs(intervals[j]), intervalBuffer.lastTs(intervals[i]));
                }

                @Override
                protected void swap(int i, int j) {
                    int tmp = intervals[i];
                    intervals[i] = intervals[j];
                    intervals[j] = tmp;
                }
            }.sort(0, intervals.length);
        }
    }

}
