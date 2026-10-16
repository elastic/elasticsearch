/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.util.List;

/** {@link AggregatorFunctionSupplier} implementation for PromQL changes over exponential histogram values. */
public final class ChangesExponentialHistogramAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return ChangesExponentialHistogramGroupingAggregatorFunction.intermediateStateDesc();
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        return new ChangesExponentialHistogramGroupingAggregatorFunction(channels, driverContext);
    }

    @Override
    public String describe() {
        return "changes of exponential histograms";
    }

    static final class ChangesExponentialHistogramGroupingAggregatorFunction extends AbstractRateGroupingFunction
        implements
            GroupingAggregatorFunction {
        private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
            new IntermediateStateDesc("timestamps", ElementType.LONG),
            new IntermediateStateDesc("values", ElementType.EXPONENTIAL_HISTOGRAM),
            new IntermediateStateDesc("sampleCounts", ElementType.LONG),
            new IntermediateStateDesc("changes", ElementType.LONG)
        );

        private final List<Integer> channels;
        private final DriverContext driverContext;
        private final BigArrays bigArrays;
        private final ExponentialHistogramRawBuffer rawBuffer;
        private final IncreaseExponentialHistogramGroupingAggregatorFunction.IntervalBuffer intervalBuffer;
        private ObjectArray<ReducedState> reducedStates;
        private int lastSliceIndex = -1;

        ChangesExponentialHistogramGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
            this.channels = channels;
            this.driverContext = driverContext;
            this.bigArrays = driverContext.bigArrays();
            ExponentialHistogramRawBuffer rawBuffer = null;
            IncreaseExponentialHistogramGroupingAggregatorFunction.IntervalBuffer intervalBuffer = null;
            try {
                rawBuffer = new ExponentialHistogramRawBuffer(driverContext.blockFactory());
                intervalBuffer = new IncreaseExponentialHistogramGroupingAggregatorFunction.IntervalBuffer(driverContext.blockFactory());
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
        public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
            // Nulls are represented by missing reduced states.
        }

        @Override
        public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
            ExponentialHistogramBlock values = page.getBlock(channels.get(0));
            if (values.areAllValuesNull()) {
                return null;
            }
            LongVector timestamps = ((LongBlock) page.getBlock(channels.get(1))).asVector();
            if (timestamps == null) {
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
            return new AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groups) {
                    addRawInput(positionOffset, groups, values, timestamps);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groups) {
                    addRawInput(positionOffset, groups, values, timestamps);
                }

                @Override
                public void add(int positionOffset, IntVector groups) {
                    addRawInput(positionOffset, groups, values, timestamps);
                }

                @Override
                public void close() {}
            };
        }

        private void addRawInput(int positionOffset, IntBlock groups, ExponentialHistogramBlock values, LongVector timestamps) {
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (groups.isNull(p) || values.isNull(valuePosition)) {
                    continue;
                }
                assert values.getValueCount(valuePosition) == 1 : "expected single-valued block " + values;
                long timestamp = timestamps.getLong(valuePosition);
                ExponentialHistogram value = values.getExponentialHistogram(values.getFirstValueIndex(valuePosition), scratch);
                int groupStart = groups.getFirstValueIndex(p);
                int groupEnd = groupStart + groups.getValueCount(p);
                for (int g = groupStart; g < groupEnd; g++) {
                    rawBuffer.append(groups.getInt(g), timestamp, value);
                }
            }
        }

        private void addRawInput(int positionOffset, IntVector groups, ExponentialHistogramBlock values, LongVector timestamps) {
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (values.isNull(valuePosition)) {
                    continue;
                }
                assert values.getValueCount(valuePosition) == 1 : "expected single-valued block " + values;
                rawBuffer.append(
                    groups.getInt(p),
                    timestamps.getLong(valuePosition),
                    values.getExponentialHistogram(values.getFirstValueIndex(valuePosition), scratch)
                );
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
            ExponentialHistogramBlock values = page.getBlock(channels.get(1));
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
            ExponentialHistogramBlock values = page.getBlock(channels.get(1));
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
                    addIntermediatePosition(
                        groups.getInt(g),
                        timestamps,
                        values,
                        sampleCount,
                        changes.getLong(valuePosition),
                        valuePosition
                    );
                }
            }
        }

        private void addIntermediatePosition(
            int groupId,
            LongBlock timestamps,
            ExponentialHistogramBlock values,
            long sampleCount,
            long changes,
            int position
        ) {
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
                ExponentialHistogramBlock.Builder values = blockFactory.newExponentialHistogramBlockBuilder(positionCount * 2);
                LongVector.FixedBuilder sampleCounts = blockFactory.newLongVectorFixedBuilder(positionCount);
                LongVector.FixedBuilder changes = blockFactory.newLongVectorFixedBuilder(positionCount)
            ) {
                ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
                for (int p = 0; p < positionCount; p++) {
                    int group = selectedInPage.getInt(p);
                    ReducedState state = group < reducedStates.size() ? reducedStates.get(group) : null;
                    // Keep intervals separate until final evaluation, matching rate's reduced-state contract.
                    if (state != null && state.samples > 0) {
                        state.writeIntervalsToBlocks(timestamps, values, scratch);
                        sampleCounts.appendLong(state.samples);
                        changes.appendLong(state.changes);
                    } else {
                        timestamps.appendLong(0);
                        values.append(ExponentialHistogram.empty());
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
            ExponentialHistogramScratch lastScratch = new ExponentialHistogramScratch();
            ExponentialHistogramScratch previousScratch = new ExponentialHistogramScratch();
            ExponentialHistogramScratch currentScratch = new ExponentialHistogramScratch();
            int sampleCount = flushQueue.valueCount;
            int lastPosition = nextPosition(flushQueue);
            ExponentialHistogram lastValue = rawBuffer.value(lastPosition, lastScratch);
            ExponentialHistogram previousValue = rawBuffer.value(lastPosition, previousScratch);
            int firstPosition = lastPosition;
            while (flushQueue.size() > 0) {
                int currentPosition = nextPosition(flushQueue);
                ExponentialHistogram currentValue = rawBuffer.value(currentPosition, currentScratch);
                if (previousValue.equals(currentValue) == false) {
                    state.changes++;
                }
                ExponentialHistogramScratch swap = previousScratch;
                previousScratch = currentScratch;
                currentScratch = swap;
                previousValue = currentValue;
                firstPosition = currentPosition;
            }
            state.samples += sampleCount;
            state.appendInterval(rawBuffer.timestamp(lastPosition), lastValue, rawBuffer.timestamp(firstPosition), previousValue);
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

            private void appendInterval(long lastTs, ExponentialHistogram lastValue, long firstTs, ExponentialHistogram firstValue) {
                int currentSize = intervals.length;
                intervals = ArrayUtil.growExact(intervals, currentSize + 1);
                intervals[currentSize] = intervalBuffer.appendInterval(lastTs, lastValue, firstTs, firstValue);
            }

            private void appendIntervalsFromBlocks(LongBlock timestamps, ExponentialHistogramBlock values, int position) {
                int intervalCount = timestamps.getValueCount(position) / 2;
                int firstIntervalId = intervalBuffer.appendIntervalsFromBlocks(timestamps, values, position);
                int currentSize = intervals.length;
                intervals = ArrayUtil.growExact(intervals, currentSize + intervalCount);
                for (int i = 0; i < intervalCount; i++) {
                    intervals[currentSize++] = firstIntervalId + i;
                }
            }

            private void writeIntervalsToBlocks(
                LongBlock.Builder timestamps,
                ExponentialHistogramBlock.Builder values,
                ExponentialHistogramScratch scratch
            ) {
                timestamps.beginPositionEntry();
                values.beginPositionEntry();
                for (int intervalId : intervals) {
                    timestamps.appendLong(intervalBuffer.lastTs(intervalId));
                    timestamps.appendLong(intervalBuffer.firstTs(intervalId));
                    values.append(intervalBuffer.lastValue(intervalId, scratch));
                    values.append(intervalBuffer.firstValue(intervalId, scratch));
                }
                timestamps.endPositionEntry();
                values.endPositionEntry();
            }

            private void combineIntervals() {
                sortIntervals();
                ExponentialHistogramScratch previousScratch = new ExponentialHistogramScratch();
                ExponentialHistogramScratch nextScratch = new ExponentialHistogramScratch();
                for (int i = 1; i < intervals.length; i++) {
                    int next = intervals[i - 1];
                    int previous = intervals[i];
                    ExponentialHistogram previousLast = intervalBuffer.lastValue(previous, previousScratch);
                    ExponentialHistogram nextFirst = intervalBuffer.firstValue(next, nextScratch);
                    if (previousLast.equals(nextFirst) == false) {
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

        private static final class ExponentialHistogramRawBuffer extends RawBuffer {
            private final ExponentialHistogramBuffer values;

            ExponentialHistogramRawBuffer(org.elasticsearch.compute.data.BlockFactory blockFactory) {
                super(blockFactory.breaker());
                boolean success = false;
                try {
                    this.values = new ExponentialHistogramBuffer(blockFactory, PAGE_SIZE);
                    success = true;
                } finally {
                    if (success == false) {
                        close();
                    }
                }
            }

            void append(int groupId, long timestamp, ExponentialHistogram value) {
                prepareSlicesOnly(groupId, timestamp);
                int newSize = timestamps.size() + 1;
                timestamps.ensureCapacity(newSize);
                values.ensureCapacity(newSize);
                timestamps.append(timestamp);
                values.append(value);
            }

            long timestamp(int position) {
                return timestamps.get(position);
            }

            ExponentialHistogram value(int position, ExponentialHistogramScratch scratch) {
                return values.get(position, scratch);
            }

            @Override
            void clearBuffers() {
                timestamps.clear();
                values.clear();
            }

            @Override
            public void close() {
                Releasables.close(values, super::close);
            }
        }
    }
}
