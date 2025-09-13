/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
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

import java.util.List;
// end generated imports

public final class RateLongGroupingAggregatorFunction implements GroupingAggregatorFunction {

    public static final class FunctionSupplier implements AggregatorFunctionSupplier {
        // Overriding constructor to support isRateOverTime flag
        private final boolean isRateOverTime;

        public FunctionSupplier(boolean isRateOverTime) {
            this.isRateOverTime = isRateOverTime;
        }

        @Override
        public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
            throw new UnsupportedOperationException("non-grouping aggregator is not supported");
        }

        @Override
        public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
            return INTERMEDIATE_STATE_DESC;
        }

        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
            throw new UnsupportedOperationException("non-grouping aggregator is not supported");
        }

        @Override
        public RateLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new RateLongGroupingAggregatorFunction(channels, driverContext, isRateOverTime);
        }

        @Override
        public String describe() {
            return "rate of long";
        }
    }

    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("timestamps", ElementType.LONG),
        new IntermediateStateDesc("values", ElementType.LONG),
        new IntermediateStateDesc("sampleCounts", ElementType.INT),
        new IntermediateStateDesc("resets", ElementType.DOUBLE)
    );

    private ObjectArray<Buffer> buffers;
    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    private ObjectArray<ReducedState> reducedStates;
    private final boolean isRateOverTime;

    public RateLongGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext, boolean isRateOverTime) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.bigArrays = driverContext.bigArrays();
        this.isRateOverTime = isRateOverTime;
        ObjectArray<Buffer> buffers = driverContext.bigArrays().newObjectArray(256);
        try {
            this.reducedStates = driverContext.bigArrays().newObjectArray(256);
            this.buffers = buffers;
            buffers = null;
        } finally {
            Releasables.close(buffers);
        }
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        // manage nulls via buffers/reducedStates arrays
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        LongBlock valuesBlock = page.getBlock(channels.get(0));
        if (valuesBlock.areAllValuesNull()) {
            return new AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {

                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {

                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {

                }

                @Override
                public void close() {

                }
            };
        }
        LongBlock timestampsBlock = page.getBlock(channels.get(1));
        LongVector timestampsVector = timestampsBlock.asVector();
        if (timestampsVector == null) {
            assert false : "expected timestamp vector in time-series aggregation";
            throw new IllegalStateException("expected timestamp vector in time-series aggregation");
        }
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                var valuesVector = valuesBlock.asVector();
                if (valuesVector != null) {
                    addRawInput(positionOffset, groupIds, valuesVector, timestampsVector);
                } else {
                    addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector);
                }
            }

            @Override
            public void close() {

            }
        };
    }

    // Note that this path can be executed randomly in tests, not in production
    private void addRawInput(int positionOffset, IntBlock groups, LongBlock valueBlock, LongVector timestampVector) {
        int lastGroup = -1;
        Buffer buffer = null;
        int positionCount = groups.getPositionCount();
        for (int p = 0; p < positionCount; p++) {
            if (groups.isNull(p)) {
                continue;
            }
            int valuePosition = p + positionOffset;
            if (valueBlock.isNull(valuePosition)) {
                continue;
            }
            assert valueBlock.getValueCount(valuePosition) == 1 : "expected single-valued block " + valueBlock;
            int groupStart = groups.getFirstValueIndex(p);
            int groupEnd = groupStart + groups.getValueCount(p);
            long timestamp = timestampVector.getLong(valuePosition);
            for (int g = groupStart; g < groupEnd; g++) {
                final int groupId = groups.getInt(g);
                final var value = valueBlock.getLong(valueBlock.getFirstValueIndex(valuePosition));
                if (lastGroup != groupId) {
                    buffer = getBuffer(groupId, 1, timestamp);
                    buffer.appendWithoutResize(timestamp, value);
                    lastGroup = groupId;
                } else {
                    buffer.maybeResizeAndAppend(bigArrays, timestamp, value);
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, LongBlock valueBlock, LongVector timestampVector) {
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            Buffer buffer = getBuffer(groupId, groups.getPositionCount(), timestampVector.getLong(0));
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (valueBlock.isNull(valuePosition)) {
                    continue;
                }
                assert valueBlock.getValueCount(valuePosition) == 1 : "expected single-valued block " + valueBlock;
                buffer.appendWithoutResize(timestampVector.getLong(valuePosition), valueBlock.getLong(valuePosition));
            }
        } else {
            int lastGroup = -1;
            Buffer buffer = null;
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (valueBlock.isNull(valuePosition) == false) {
                    continue;
                }
                assert valueBlock.getValueCount(valuePosition) == 1 : "expected single-valued block " + valueBlock;
                long timestamp = timestampVector.getLong(valuePosition);
                var value = valueBlock.getLong(valuePosition);
                int groupId = groups.getInt(p);
                if (lastGroup != groupId) {
                    buffer = getBuffer(groupId, 1, timestamp);
                    buffer.appendWithoutResize(timestamp, value);
                    lastGroup = groupId;
                } else {
                    buffer.maybeResizeAndAppend(bigArrays, timestamp, value);
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, LongVector valueVector, LongVector timestampVector) {
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            Buffer buffer = getBuffer(groupId, positionCount, timestampVector.getLong(0));
            for (int p = 0; p < positionCount; p++) {
                int valuePosition = positionOffset + p;
                buffer.appendWithoutResize(timestampVector.getLong(valuePosition), valueVector.getLong(valuePosition));
            }
        } else {
            int lastGroup = -1;
            Buffer buffer = null;
            for (int p = 0; p < positionCount; p++) {
                int valuePosition = positionOffset + p;
                long timestamp = timestampVector.getLong(valuePosition);
                var value = valueVector.getLong(valuePosition);
                int groupId = groups.getInt(p);
                if (lastGroup != groupId) {
                    buffer = getBuffer(groupId, 1, timestamp);
                    buffer.appendWithoutResize(timestamp, value);
                    lastGroup = groupId;
                } else {
                    buffer.maybeResizeAndAppend(bigArrays, timestamp, value);
                }
            }
        }
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
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
        LongBlock values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        IntVector sampleCounts = ((IntBlock) page.getBlock(channels.get(2))).asVector();
        DoubleVector resets = ((DoubleBlock) page.getBlock(channels.get(3))).asVector();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            int sampleCount = sampleCounts.getInt(valuePosition);
            if (sampleCount == 0) {
                continue;
            }
            int groupId = groups.getInt(groupPosition);
            reducedStates = bigArrays.grow(reducedStates, groupId + 1);
            ReducedState state = reducedStates.get(groupId);
            if (state == null) {
                state = new ReducedState();
                reducedStates.set(groupId, state);
            }
            state.appendValuesFromBlocks(timestamps, values, valuePosition);
            state.samples += sampleCount;
            state.resets += resets.getDouble(valuePosition);
        }
    }

    private void addIntermediateInputBlock(int positionOffset, IntBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        LongBlock timestamps = page.getBlock(channels.get(0));
        LongBlock values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        IntVector sampleCounts = ((IntBlock) page.getBlock(channels.get(2))).asVector();
        DoubleVector resets = ((DoubleBlock) page.getBlock(channels.get(3))).asVector();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            int sampleCount = sampleCounts.getInt(valuePosition);
            if (sampleCount == 0) {
                continue;
            }
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int firstGroup = groups.getFirstValueIndex(groupPosition);
            int lastGroup = firstGroup + groups.getValueCount(groupPosition);
            for (int g = firstGroup; g < lastGroup; g++) {
                int groupId = groups.getInt(g);
                reducedStates = bigArrays.grow(reducedStates, groupId + 1);
                ReducedState state = reducedStates.get(groupId);
                if (state == null) {
                    state = new ReducedState();
                    reducedStates.set(groupId, state);
                }
                state.appendValuesFromBlocks(timestamps, values, valuePosition);
                state.samples += sampleCount;
                state.resets += resets.getDouble(groupPosition);
            }
        }
    }

    @Override
    public final void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (
            var timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
            var values = blockFactory.newLongBlockBuilder(positionCount * 2);
            var sampleCounts = blockFactory.newIntVectorFixedBuilder(positionCount);
            var resets = blockFactory.newDoubleVectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushAndCombineState(group);
                if (state != null && state.timestamps.length > 0) {
                    if (state.samples > 1) {
                        timestamps.beginPositionEntry();
                        values.beginPositionEntry();
                        for (int s = 0; s < state.timestamps.length; s++) {
                            timestamps.appendLong(state.timestamps[s]);
                            values.appendLong(state.values[s]);
                        }
                        timestamps.endPositionEntry();
                        values.endPositionEntry();
                    } else {
                        timestamps.appendLong(state.timestamps[0]);
                        values.appendLong(state.values[0]);
                    }
                    sampleCounts.appendInt(state.samples);
                    resets.appendDouble(state.resets);
                } else {
                    timestamps.appendLong(0);
                    values.appendLong(0);
                    sampleCounts.appendInt(0);
                    resets.appendDouble(0);
                }
            }
            blocks[offset] = timestamps.build();
            blocks[offset + 1] = values.build();
            blocks[offset + 2] = sampleCounts.build().asBlock();
            blocks[offset + 3] = resets.build().asBlock();
        }
    }

    @Override
    public void close() {
        for (long i = 0; i < buffers.size(); i++) {
            Buffer buffer = buffers.get(i);
            if (buffer != null) {
                buffer.close();
            }
        }
        Releasables.close(reducedStates, buffers);
    }

    private Buffer getBuffer(int groupId, int newElements, long firstTimestamp) {
        buffers = bigArrays.grow(buffers, groupId + 1);
        Buffer buffer = buffers.get(groupId);
        if (buffer == null) {
            buffer = new Buffer(bigArrays, newElements);
            buffers.set(groupId, buffer);
        } else {
            buffer.ensureCapacity(bigArrays, newElements, firstTimestamp);
        }
        return buffer;
    }

    /**
     * Buffers data points in two arrays: one for timestamps and one for values, partitioned into multiple slices.
     * Each slice is sorted in descending order of timestamp. A new slice is created when a data point has a
     * timestamp greater than the last point of the current slice. Since each page is sorted by descending timestamp,
     * we only need to compare the first point of the new page with the last point of the current slice to decide
     * if a new slice is needed. During merging, a priority queue is used to iterate through the slices, selecting
     * the slice with the greatest timestamp.
     */
    static final class Buffer implements Releasable {
        private LongArray timestamps;
        private LongArray values;
        private int pendingCount;
        int[] sliceOffsets;
        private static final int[] EMPTY_SLICES = new int[0];

        Buffer(BigArrays bigArrays, int initialSize) {
            this.timestamps = bigArrays.newLongArray(Math.max(initialSize, 32), false);
            this.values = bigArrays.newLongArray(Math.max(initialSize, 32), false);
            this.sliceOffsets = EMPTY_SLICES;
        }

        void appendWithoutResize(long timestamp, long value) {
            timestamps.set(pendingCount, timestamp);
            values.set(pendingCount, value);
            pendingCount++;
        }

        void maybeResizeAndAppend(BigArrays bigArrays, long timestamp, long value) {
            timestamps = bigArrays.grow(timestamps, pendingCount + 1);
            values = bigArrays.grow(values, pendingCount + 1);

            timestamps.set(pendingCount, timestamp);
            values.set(pendingCount, value);
            pendingCount++;
        }

        void ensureCapacity(BigArrays bigArrays, int count, long firstTimestamp) {
            int newSize = pendingCount + count;
            timestamps = bigArrays.grow(timestamps, newSize);
            values = bigArrays.grow(values, newSize);
            if (pendingCount > 0 && firstTimestamp > timestamps.get(pendingCount - 1)) {
                if (sliceOffsets.length == 0 || sliceOffsets[sliceOffsets.length - 1] != pendingCount) {
                    sliceOffsets = ArrayUtil.growExact(sliceOffsets, sliceOffsets.length + 1);
                    sliceOffsets[sliceOffsets.length - 1] = pendingCount;
                }
            }
        }

        void flush(ReducedState state) {
            if (pendingCount == 0) {
                return;
            }
            if (pendingCount == 1) {
                state.samples++;
                state.appendOneValue(timestamps.get(0), values.get(0));
                return;
            }
            PriorityQueue<Slice> pq = mergeQueue();
            // first
            final long lastTimestamp;
            final long lastValue;
            {
                Slice top = pq.top();
                lastTimestamp = top.timestamp;
                int position = top.next();
                lastValue = values.get(position);
                if (top.exhausted()) {
                    pq.pop();
                } else {
                    pq.updateTop();
                }
            }
            var prevValue = lastValue;
            double reset = 0;
            int position = -1;
            while (pq.size() > 0) {
                Slice top = pq.top();
                position = top.next();
                if (top.exhausted()) {
                    pq.pop();
                } else {
                    pq.updateTop();
                }
                var val = values.get(position);
                reset += dv(val, prevValue) + dv(prevValue, lastValue) - dv(val, lastValue);
                prevValue = val;
            }
            state.samples += pendingCount;
            state.resets += reset;
            state.appendTwoValues(lastTimestamp, lastValue, timestamps.get(position), prevValue);
        }

        private PriorityQueue<Slice> mergeQueue() {
            PriorityQueue<Slice> pq = new PriorityQueue<>(this.sliceOffsets.length + 1) {
                @Override
                protected boolean lessThan(Slice a, Slice b) {
                    return a.timestamp > b.timestamp; // want the latest timestamp first
                }
            };
            int startOffset = 0;
            for (int sliceOffset : sliceOffsets) {
                pq.add(new Slice(this, startOffset, sliceOffset));
                startOffset = sliceOffset;
            }
            pq.add(new Slice(this, startOffset, pendingCount));
            return pq;
        }

        @Override
        public void close() {
            timestamps.close();
            values.close();
        }
    }

    static final class Slice {
        int start;
        long timestamp;
        final int end;
        final Buffer buffer;

        Slice(Buffer buffer, int start, int end) {
            this.buffer = buffer;
            this.start = start;
            this.end = end;
            this.timestamp = buffer.timestamps.get(start);
        }

        boolean exhausted() {
            return start >= end;
        }

        int next() {
            int index = start++;
            if (start < end) {
                timestamp = buffer.timestamps.get(start);
            }
            return index;
        }
    }

    @Override
    public final void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (var rates = blockFactory.newDoubleBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushAndCombineState(group);
                if (state == null || state.timestamps.length < 2) {
                    rates.appendNull();
                    continue;
                }
                final double rate;
                if (evalContext instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                    rate = extrapolateRate(state, tsContext.rangeStartInMillis(group), tsContext.rangeEndInMillis(group), isRateOverTime);
                } else {
                    rate = computeRateWithoutExtrapolate(state);
                }
                rates.appendDouble(rate);
            }
            blocks[offset] = rates.build();
        }
    }

    ReducedState flushAndCombineState(int groupId) {
        ReducedState state = groupId < reducedStates.size() ? reducedStates.getAndSet(groupId, null) : null;
        Buffer buffer = groupId < buffers.size() ? buffers.getAndSet(groupId, null) : null;
        if (buffer != null) {
            try (buffer) {
                if (state == null) {
                    state = new ReducedState();
                }
                buffer.flush(state);
            }
        }
        return state;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    static final class ReducedState {
        private static final long[] EMPTY_LONGS = new long[0];
        private static final long[] EMPTY_VALUES = new long[0];
        int samples;
        double resets;
        long[] timestamps = EMPTY_LONGS;
        long[] values = EMPTY_VALUES;

        void appendOneValue(long t, long v) {
            int currentSize = timestamps.length;
            this.timestamps = ArrayUtil.growExact(timestamps, currentSize + 1);
            this.values = ArrayUtil.growExact(values, currentSize + 1);
            this.timestamps[currentSize] = t;
            this.values[currentSize] = v;
        }

        void appendTwoValues(long t1, long v1, long t2, long v2) {
            int currentSize = timestamps.length;
            this.timestamps = ArrayUtil.growExact(timestamps, currentSize + 2);
            this.values = ArrayUtil.growExact(values, currentSize + 2);
            this.timestamps[currentSize] = t1;
            this.values[currentSize] = v1;
            currentSize++;
            this.timestamps[currentSize] = t2;
            this.values[currentSize] = v2;
        }

        void appendValuesFromBlocks(LongBlock ts, LongBlock vs, int position) {
            int tsFirst = ts.getFirstValueIndex(position);
            int vsFirst = vs.getFirstValueIndex(position);
            int count = ts.getValueCount(position);
            int total = timestamps.length + count;
            long[] mergedTimestamps = new long[total];
            long[] mergedValues = new long[total];
            int i = 0, j = 0, k = 0;
            while (i < timestamps.length && j < count) {
                long t = ts.getLong(tsFirst + j);
                if (timestamps[i] > t) {
                    mergedTimestamps[k] = timestamps[i];
                    mergedValues[k++] = values[i++];
                } else {
                    mergedTimestamps[k] = t;
                    mergedValues[k++] = vs.getLong(vsFirst + j++);
                }
            }
            while (i < timestamps.length) {
                mergedTimestamps[k] = timestamps[i];
                mergedValues[k++] = values[i++];
            }
            while (j < count) {
                mergedTimestamps[k] = ts.getLong(tsFirst + j);
                mergedValues[k++] = vs.getLong(vsFirst + j++);
            }
            this.timestamps = mergedTimestamps;
            this.values = mergedValues;
        }
    }

    private static double computeRateWithoutExtrapolate(ReducedState state) {
        final int len = state.timestamps.length;
        assert len >= 2 : "rate requires at least two samples; got " + len;
        final long firstTS = state.timestamps[state.timestamps.length - 1];
        final long lastTS = state.timestamps[0];
        double reset = state.resets;
        for (int i = 1; i < len; i++) {
            if (state.values[i - 1] < state.values[i]) {
                reset += state.values[i];
            }
        }
        final double firstValue = state.values[len - 1];
        final double lastValue = state.values[0] + reset;
        return (lastValue - firstValue) * 1000.0 / (lastTS - firstTS);
    }

    /**
     * Credit to PromQL for this extrapolation algorithm:
     * If samples are close enough to the rangeStart and rangeEnd, we extrapolate the rate all the way to the boundary in question.
     * "Close enough" is defined as "up to 10% more than the average duration between samples within the range".
     * Essentially, we assume a more or less regular spacing between samples. If we don't see a sample where we would expect one,
     * we assume the series does not cover the whole range but starts and/or ends within the range.
     * We still extrapolate the rate in this case, but not all the way to the boundary, only by half of the average duration between
     * samples (which is our guess for where the series actually starts or ends).
     */
    private static double extrapolateRate(ReducedState state, long rangeStart, long rangeEnd, boolean isRateOverTime) {
        final int len = state.timestamps.length;
        assert len >= 2 : "rate requires at least two samples; got " + len;
        final long firstTS = state.timestamps[state.timestamps.length - 1];
        final long lastTS = state.timestamps[0];
        double reset = state.resets;
        for (int i = 1; i < len; i++) {
            if (state.values[i - 1] < state.values[i]) {
                reset += state.values[i];
            }
        }
        double firstValue = state.values[len - 1];
        double lastValue = state.values[0] + reset;
        final double sampleTS = lastTS - firstTS;
        final double averageSampleInterval = sampleTS / state.samples;
        final double slope = (lastValue - firstValue) / sampleTS;
        double startGap = firstTS - rangeStart;
        if (startGap > 0) {
            if (startGap > averageSampleInterval * 1.1) {
                startGap = averageSampleInterval / 2.0;
            }
            firstValue = Math.max(0.0, firstValue - startGap * slope);
        }
        double endGap = rangeEnd - lastTS;
        if (endGap > 0) {
            if (endGap > averageSampleInterval * 1.1) {
                endGap = averageSampleInterval / 2.0;
            }
            lastValue = lastValue + endGap * slope;
        }
        if (isRateOverTime) {
            return (lastValue - firstValue) * 1000.0 / (rangeEnd - rangeStart);
        } else {
            return lastValue - firstValue;
        }
    }

    // TODO: copied from old rate - simplify this or explain why we need it?
    static double dv(double v0, double v1) {
        return v0 > v1 ? v1 : v1 - v0;
    }
}
