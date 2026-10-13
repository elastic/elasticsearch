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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

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
        private final IntervalBuffer intervalBuffer;
        private ObjectArray<ReducedState> reducedStates;

        ChangesExponentialHistogramGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
            this.channels = channels;
            this.driverContext = driverContext;
            this.bigArrays = driverContext.bigArrays();
            ExponentialHistogramRawBuffer rawBuffer = null;
            IntervalBuffer intervalBuffer = null;
            try {
                rawBuffer = new ExponentialHistogramRawBuffer(driverContext.blockFactory());
                intervalBuffer = new IntervalBuffer(driverContext.blockFactory());
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
            LongBlock timestamps = page.getBlock(channels.get(1));
            if (values.areAllValuesNull() || timestamps.areAllValuesNull()) {
                return null;
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

        private void addRawInput(int positionOffset, IntBlock groups, ExponentialHistogramBlock values, LongBlock timestamps) {
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (groups.isNull(p) || values.isNull(valuePosition) || timestamps.isNull(valuePosition)) {
                    continue;
                }
                int valueStart = values.getFirstValueIndex(valuePosition);
                int valueEnd = valueStart + values.getValueCount(valuePosition);
                int timestampStart = timestamps.getFirstValueIndex(valuePosition);
                int timestampEnd = timestampStart + timestamps.getValueCount(valuePosition);
                int groupStart = groups.getFirstValueIndex(p);
                int groupEnd = groupStart + groups.getValueCount(p);
                for (int g = groupStart; g < groupEnd; g++) {
                    appendValues(groups.getInt(g), timestamps, timestampStart, timestampEnd, values, valueStart, valueEnd, scratch);
                }
            }
        }

        private void addRawInput(int positionOffset, IntVector groups, ExponentialHistogramBlock values, LongBlock timestamps) {
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (values.isNull(valuePosition) || timestamps.isNull(valuePosition)) {
                    continue;
                }
                int valueStart = values.getFirstValueIndex(valuePosition);
                int valueEnd = valueStart + values.getValueCount(valuePosition);
                int timestampStart = timestamps.getFirstValueIndex(valuePosition);
                int timestampEnd = timestampStart + timestamps.getValueCount(valuePosition);
                appendValues(groups.getInt(p), timestamps, timestampStart, timestampEnd, values, valueStart, valueEnd, scratch);
            }
        }

        private void appendValues(
            int groupId,
            LongBlock timestamps,
            int timestampStart,
            int timestampEnd,
            ExponentialHistogramBlock values,
            int valueStart,
            int valueEnd,
            ExponentialHistogramScratch scratch
        ) {
            for (int t = timestampStart; t < timestampEnd; t++) {
                long timestamp = timestamps.getLong(t);
                for (int v = valueStart; v < valueEnd; v++) {
                    rawBuffer.append(groupId, timestamp, values.getExponentialHistogram(v, scratch));
                }
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
                FlushQueue flushQueue = flushQueues.getFlushQueue(groupId, rawBuffer::compareValues);
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
                if (histogramsEqual(previousValue, currentValue) == false) {
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

        private static int compareHistograms(ExponentialHistogram left, ExponentialHistogram right) {
            if (histogramsEqual(left, right)) {
                return 0;
            }
            int result = Integer.compare(left.scale(), right.scale());
            if (result == 0) {
                result = Long.compare(left.valueCount(), right.valueCount());
            }
            if (result == 0) {
                result = compareRawDouble(left.sum(), right.sum());
            }
            if (result == 0) {
                result = compareZeroBuckets(left.zeroBucket(), right.zeroBucket());
            }
            if (result == 0) {
                result = compareBuckets(left.negativeBuckets().iterator(), right.negativeBuckets().iterator());
            }
            if (result == 0) {
                result = compareBuckets(left.positiveBuckets().iterator(), right.positiveBuckets().iterator());
            }
            return result;
        }

        private static boolean histogramsEqual(ExponentialHistogram left, ExponentialHistogram right) {
            return left.scale() == right.scale()
                && left.valueCount() == right.valueCount()
                && Double.doubleToRawLongBits(left.sum()) == Double.doubleToRawLongBits(right.sum())
                && left.zeroBucket().zeroThreshold() == right.zeroBucket().zeroThreshold()
                && left.zeroBucket().count() == right.zeroBucket().count()
                && compareBuckets(left.negativeBuckets().iterator(), right.negativeBuckets().iterator()) == 0
                && compareBuckets(left.positiveBuckets().iterator(), right.positiveBuckets().iterator()) == 0;
        }

        private static int compareRawDouble(double left, double right) {
            int result = Double.compare(left, right);
            if (result == 0) {
                result = Long.compareUnsigned(Double.doubleToRawLongBits(left), Double.doubleToRawLongBits(right));
            }
            return result;
        }

        private static int compareZeroBuckets(ZeroBucket left, ZeroBucket right) {
            int result = left.zeroThreshold() == right.zeroThreshold() ? 0 : Double.compare(left.zeroThreshold(), right.zeroThreshold());
            if (result == 0) {
                result = Long.compare(left.count(), right.count());
            }
            return result;
        }

        private static int compareBuckets(BucketIterator left, BucketIterator right) {
            int result = Integer.compare(left.scale(), right.scale());
            while (result == 0 && left.hasNext() && right.hasNext()) {
                result = Long.compare(left.peekIndex(), right.peekIndex());
                if (result == 0) {
                    result = Long.compare(left.peekCount(), right.peekCount());
                }
                left.advance();
                right.advance();
            }
            if (result == 0) {
                result = Boolean.compare(left.hasNext(), right.hasNext());
            }
            return result;
        }

        @Override
        public void close() {
            Releasables.close(reducedStates, rawBuffer, intervalBuffer);
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
                    if (histogramsEqual(previousLast, nextFirst) == false) {
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

        private static final class IntervalBuffer implements Releasable {
            private final LongBuffer timestamps;
            private final ExponentialHistogramBuffer values;

            private IntervalBuffer(BlockFactory blockFactory) {
                LongBuffer timestamps = null;
                ExponentialHistogramBuffer values = null;
                boolean success = false;
                try {
                    timestamps = new LongBuffer(blockFactory.breaker(), PAGE_SIZE);
                    values = new ExponentialHistogramBuffer(blockFactory, PAGE_SIZE);
                    success = true;
                } finally {
                    if (success == false) {
                        Releasables.close(timestamps, values);
                    }
                }
                this.timestamps = timestamps;
                this.values = values;
            }

            private int count() {
                return timestamps.size() / 2;
            }

            private long lastTs(int intervalId) {
                return timestamps.get(2 * intervalId);
            }

            private ExponentialHistogram lastValue(int intervalId, ExponentialHistogramScratch scratch) {
                return values.get(2 * intervalId, scratch);
            }

            private long firstTs(int intervalId) {
                return timestamps.get(2 * intervalId + 1);
            }

            private ExponentialHistogram firstValue(int intervalId, ExponentialHistogramScratch scratch) {
                return values.get(2 * intervalId + 1, scratch);
            }

            private int appendInterval(long lastTs, ExponentialHistogram lastValue, long firstTs, ExponentialHistogram firstValue) {
                int intervalId = count();
                timestamps.ensureCapacity(timestamps.size() + 2);
                values.ensureCapacity(values.size() + 2);
                timestamps.append(lastTs);
                values.append(lastValue);
                timestamps.append(firstTs);
                values.append(firstValue);
                return intervalId;
            }

            private int appendIntervalsFromBlocks(LongBlock timestampBlock, ExponentialHistogramBlock valueBlock, int position) {
                int timestampStart = timestampBlock.getFirstValueIndex(position);
                int valueStart = valueBlock.getFirstValueIndex(position);
                int valueCount = timestampBlock.getValueCount(position);
                assert valueCount == valueBlock.getValueCount(position);
                assert valueCount % 2 == 0 : "expected interval endpoints but got [" + valueCount + "] values";
                timestamps.ensureCapacity(timestamps.size() + valueCount);
                values.ensureCapacity(values.size() + valueCount);
                ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
                int firstIntervalId = count();
                for (int i = 0; i < valueCount; i++) {
                    timestamps.append(timestampBlock.getLong(timestampStart + i));
                    values.append(valueBlock.getExponentialHistogram(valueStart + i, scratch));
                }
                return firstIntervalId;
            }

            @Override
            public void close() {
                Releasables.close(timestamps, values);
            }
        }

        private static final class ExponentialHistogramRawBuffer extends RawBuffer {
            private final ExponentialHistogramBuffer values;
            private final ExponentialHistogramScratch leftScratch = new ExponentialHistogramScratch();
            private final ExponentialHistogramScratch rightScratch = new ExponentialHistogramScratch();

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

            int compareValues(int leftPosition, int rightPosition) {
                ExponentialHistogram left = value(leftPosition, leftScratch);
                ExponentialHistogram right = value(rightPosition, rightScratch);
                return compareHistograms(left, right);
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
