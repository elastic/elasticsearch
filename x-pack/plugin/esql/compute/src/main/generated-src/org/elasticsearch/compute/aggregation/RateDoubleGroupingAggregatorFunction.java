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

public final class RateDoubleGroupingAggregatorFunction implements GroupingAggregatorFunction {

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
        public RateDoubleGroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new RateDoubleGroupingAggregatorFunction(channels, driverContext, isRateOverTime);
        }

        @Override
        public String describe() {
            return "rate of double";
        }
    }

    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("timestamps", ElementType.LONG),
        new IntermediateStateDesc("values", ElementType.DOUBLE),
        new IntermediateStateDesc("sampleCounts", ElementType.LONG),
        new IntermediateStateDesc("resets", ElementType.DOUBLE)
    );

    private ObjectArray<Buffer> buffers;
    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    private ObjectArray<ReducedState> reducedStates;
    private final boolean isRateOverTime;

    public RateDoubleGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext, boolean isRateOverTime) {
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
        DoubleBlock valuesBlock = page.getBlock(channels.get(0));
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
        IntVector sliceIndices = ((IntBlock) page.getBlock(channels.get(2))).asVector();
        assert sliceIndices != null : "expected slice indices vector in time-series aggregation";
        LongVector futureMaxTimestamps = ((LongBlock) page.getBlock(channels.get(3))).asVector();
        assert futureMaxTimestamps != null : "expected future max timestamps vector in time-series aggregation";

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
    private void addRawInput(int positionOffset, IntBlock groups, DoubleBlock valueBlock, LongVector timestampVector) {
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
                final var value = valueBlock.getDouble(valueBlock.getFirstValueIndex(valuePosition));
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

    private void addRawInput(int positionOffset, IntVector groups, DoubleBlock valueBlock, LongVector timestampVector) {
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            addSubRange(groupId, positionOffset, positionOffset + positionCount, valueBlock, timestampVector);
        } else {
            int lastGroup = groups.getInt(0);
            int lastPosition = 0;
            for (int p = 1; p < positionCount; p++) {
                int group = groups.getInt(p);
                if (group != lastGroup) {
                    addSubRange(lastGroup, positionOffset + lastPosition, positionOffset + p, valueBlock, timestampVector);
                    lastGroup = group;
                    lastPosition = p;
                }
            }
            addSubRange(lastGroup, positionOffset + lastPosition, positionOffset + positionCount, valueBlock, timestampVector);
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, DoubleVector valueVector, LongVector timestampVector) {
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            addSubRange(groupId, positionOffset, positionOffset + positionCount, valueVector, timestampVector);
        } else {
            int lastGroup = groups.getInt(0);
            int lastPosition = 0;
            for (int p = 1; p < positionCount; p++) {
                int group = groups.getInt(p);
                if (group != lastGroup) {
                    addSubRange(lastGroup, positionOffset + lastPosition, positionOffset + p, valueVector, timestampVector);
                    lastGroup = group;
                    lastPosition = p;
                }
            }
            addSubRange(lastGroup, positionOffset + lastPosition, positionOffset + positionCount, valueVector, timestampVector);
        }
    }

    private void addSubRange(int group, int from, int to, DoubleVector valueVector, LongVector timestampVector) {
        var buffer = getBuffer(group, to - from, timestampVector.getLong(from));
        buffer.appendRange(from, to, valueVector, timestampVector);
    }

    private void addSubRange(int group, int from, int to, DoubleBlock valueBlock, LongVector timestampVector) {
        var buffer = getBuffer(group, to - from, timestampVector.getLong(from));
        buffer.appendRange(from, to, valueBlock, timestampVector);
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
        DoubleBlock values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        LongVector sampleCounts = ((LongBlock) page.getBlock(channels.get(2))).asVector();
        DoubleVector resets = ((DoubleBlock) page.getBlock(channels.get(3))).asVector();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            long sampleCount = sampleCounts.getLong(valuePosition);
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
            state.appendIntervalsFromBlocks(timestamps, values, valuePosition);
            state.samples += sampleCount;
            state.resets += resets.getDouble(valuePosition);
        }
    }

    private void addIntermediateInputBlock(int positionOffset, IntBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        LongBlock timestamps = page.getBlock(channels.get(0));
        DoubleBlock values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        LongVector sampleCounts = ((LongBlock) page.getBlock(channels.get(2))).asVector();
        DoubleVector resets = ((DoubleBlock) page.getBlock(channels.get(3))).asVector();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            long sampleCount = sampleCounts.getLong(valuePosition);
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
                state.appendIntervalsFromBlocks(timestamps, values, valuePosition);
                state.samples += sampleCount;
                state.resets += resets.getDouble(valuePosition);
            }
        }
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (
            var timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
            var values = blockFactory.newDoubleBlockBuilder(positionCount * 2);
            var sampleCounts = blockFactory.newLongVectorFixedBuilder(positionCount);
            var resets = blockFactory.newDoubleVectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushAndCombineState(group);
                // Do not combine intervals across shards because intervals from different indices may overlap.
                if (state != null && state.samples > 0) {
                    timestamps.beginPositionEntry();
                    values.beginPositionEntry();
                    for (Interval interval : state.intervals) {
                        timestamps.appendLong(interval.t1);
                        timestamps.appendLong(interval.t2);
                        values.appendDouble(interval.v1);
                        values.appendDouble(interval.v2);
                    }
                    timestamps.endPositionEntry();
                    values.endPositionEntry();
                    sampleCounts.appendLong(state.samples);
                    resets.appendDouble(state.resets);
                } else {
                    timestamps.appendLong(0);
                    values.appendDouble(0);
                    sampleCounts.appendLong(0);
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
        private DoubleArray values;
        private int pendingCount;
        int[] sliceOffsets;
        private static final int[] EMPTY_SLICES = new int[0];

        Buffer(BigArrays bigArrays, int initialSize) {
            this.timestamps = bigArrays.newLongArray(Math.max(initialSize, 32), false);
            this.values = bigArrays.newDoubleArray(Math.max(initialSize, 32), false);
            this.sliceOffsets = EMPTY_SLICES;
        }

        void appendWithoutResize(long timestamp, double value) {
            timestamps.set(pendingCount, timestamp);
            values.set(pendingCount, value);
            pendingCount++;
        }

        void maybeResizeAndAppend(BigArrays bigArrays, long timestamp, double value) {
            timestamps = bigArrays.grow(timestamps, pendingCount + 1);
            values = bigArrays.grow(values, pendingCount + 1);

            timestamps.set(pendingCount, timestamp);
            values.set(pendingCount, value);
            pendingCount++;
        }

        void appendRange(int fromPosition, int toPosition, DoubleVector valueVector, LongVector timestampVector) {
            for (int p = fromPosition; p < toPosition; p++) {
                values.set(pendingCount, valueVector.getDouble(p));
                timestamps.set(pendingCount, timestampVector.getLong(p));
                pendingCount++;
            }
        }

        void appendRange(int fromPosition, int toPosition, DoubleBlock valueBlock, LongVector timestampVector) {
            for (int p = fromPosition; p < toPosition; p++) {
                if (valueBlock.isNull(p)) {
                    continue;
                }
                assert valueBlock.getValueCount(p) == 1 : "expected single-valued block " + valueBlock;
                values.set(pendingCount, valueBlock.getDouble(p));
                timestamps.set(pendingCount, timestampVector.getLong(p));
                pendingCount++;
            }
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
                long t = timestamps.get(0);
                double v = values.get(0);
                state.appendInterval(new Interval(t, v, t, v));
                return;
            }
            PriorityQueue<Slice> pq = mergeQueue();
            // first
            final long lastTimestamp;
            final double lastValue;
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
                if (val > prevValue) {
                    state.resets += val;
                }
                prevValue = val;
            }
            state.samples += pendingCount;
            state.appendInterval(new Interval(lastTimestamp, lastValue, timestamps.get(position), prevValue));
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
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (var rates = blockFactory.newDoubleBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushAndCombineState(group);
                if (state == null || state.samples < 2) {
                    rates.appendNull();
                    continue;
                }
                // combine intervals for the final evaluation
                Interval[] intervals = state.intervals;
                ArrayUtil.timSort(intervals);
                for (int i = 1; i < intervals.length; i++) {
                    Interval next = intervals[i - 1]; // reversed
                    Interval prev = intervals[i];
                    if (prev.v1 > next.v2) {
                        state.resets += prev.v1;
                    }
                }
                final double rate;
                if (evalContext instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                    rate = extrapolateRate(state, tsContext.rangeStartInMillis(group), tsContext.rangeEndInMillis(group), isRateOverTime);
                } else {
                    rate = computeRateWithoutExtrapolate(state, isRateOverTime);
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

    record Interval(long t1, double v1, long t2, double v2) implements Comparable<Interval> {
        @Override
        public int compareTo(Interval other) {
            return Long.compare(other.t1, t1); // want most recent first
        }
    }

    static final class ReducedState {
        private static final Interval[] EMPTY_INTERVALS = new Interval[0];
        long samples;
        double resets;
        Interval[] intervals = EMPTY_INTERVALS;

        void appendInterval(Interval interval) {
            int currentSize = intervals.length;
            this.intervals = ArrayUtil.growExact(intervals, currentSize + 1);
            this.intervals[currentSize] = interval;
        }

        void appendIntervalsFromBlocks(LongBlock ts, DoubleBlock vs, int position) {
            int tsFirst = ts.getFirstValueIndex(position);
            int vsFirst = vs.getFirstValueIndex(position);
            int count = ts.getValueCount(position);
            assert count % 2 == 0 : "expected even number of values for intervals, got " + count + " in " + ts;
            int currentSize = intervals.length;
            intervals = ArrayUtil.growExact(intervals, currentSize + (count / 2));
            for (int i = 0; i < count; i += 2) {
                Interval interval = new Interval(
                    ts.getLong(tsFirst + i),
                    vs.getDouble(vsFirst + i),
                    ts.getLong(tsFirst + i + 1),
                    vs.getDouble(vsFirst + i + 1)
                );
                intervals[currentSize++] = interval;
            }
        }
    }

    private static double computeRateWithoutExtrapolate(ReducedState state, boolean isRateOverTime) {
        assert state.samples >= 2 : "rate requires at least two samples; got " + state.samples;
        final long firstTS = state.intervals[state.intervals.length - 1].t2;
        final long lastTS = state.intervals[0].t1;
        double firstValue = state.intervals[state.intervals.length - 1].v2;
        double lastValue = state.intervals[0].v1 + state.resets;
        if (isRateOverTime) {
            return (lastValue - firstValue) * 1000.0 / (lastTS - firstTS);
        } else {
            return lastValue - firstValue;
        }
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
        assert state.samples >= 2 : "rate requires at least two samples; got " + state.samples;
        final long firstTS = state.intervals[state.intervals.length - 1].t2;
        final long lastTS = state.intervals[0].t1;
        double firstValue = state.intervals[state.intervals.length - 1].v2;
        double lastValue = state.intervals[0].v1 + state.resets;
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
}
