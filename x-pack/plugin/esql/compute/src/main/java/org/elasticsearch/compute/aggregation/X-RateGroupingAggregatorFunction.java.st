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
import org.elasticsearch.common.util.LongObjectPagedHashMap;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
// end generated imports

public final class Rate$Type$GroupingAggregatorFunction implements GroupingAggregatorFunction {

    public static final class FunctionSupplier implements AggregatorFunctionSupplier {
        // Overriding constructor to support isRateOverTime flag
        private final boolean isRateOverTime;
        private final boolean isDateNanos;

        public FunctionSupplier(boolean isRateOverTime, boolean isDateNanos) {
            this.isRateOverTime = isRateOverTime;
            this.isDateNanos = isDateNanos;
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
        public Rate$Type$GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new Rate$Type$GroupingAggregatorFunction(channels, driverContext, isRateOverTime, isDateNanos);
        }

        @Override
        public String describe() {
            return "rate of $type$";
        }
    }

    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("timestamps", ElementType.LONG),
        new IntermediateStateDesc("values", ElementType.$TYPE$),
        new IntermediateStateDesc("sampleCounts", ElementType.LONG),
        new IntermediateStateDesc("resets", ElementType.DOUBLE)
    );

    private ObjectArray<Buffer> buffers;
    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    private ObjectArray<ReducedState> reducedStates;
    private final boolean isRateOverTime;
    private final double dateFactor;

    public Rate$Type$GroupingAggregatorFunction(
        List<Integer> channels,
        DriverContext driverContext,
        boolean isRateOverTime,
        boolean isDateNanos
    ) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.bigArrays = driverContext.bigArrays();
        this.isRateOverTime = isRateOverTime;
        ObjectArray<Buffer> buffers = driverContext.bigArrays().newObjectArray(256);
        this.dateFactor = isDateNanos ? 1_000_000_000.0 : 1000.0;
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
        $Type$Block valuesBlock = page.getBlock(channels.get(0));
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
    private void addRawInput(int positionOffset, IntBlock groups, $Type$Block valueBlock, LongVector timestampVector) {
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
                final var value = valueBlock.get$Type$(valueBlock.getFirstValueIndex(valuePosition));
                if (lastGroup != groupId) {
                    buffer = getBuffer(groupId);
                    buffer.append(timestamp, value);
                    lastGroup = groupId;
                } else {
                    buffer.append(timestamp, value);
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, $Type$Block valueBlock, LongVector timestampVector) {
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

    private void addRawInput(int positionOffset, IntVector groups, $Type$Vector valueVector, LongVector timestampVector) {
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

    private void addSubRange(int group, int from, int to, $Type$Vector valueVector, LongVector timestampVector) {
        var buffer = getBuffer(group);
        buffer.appendRange(from, to, valueVector, timestampVector);
    }

    private void addSubRange(int group, int from, int to, $Type$Block valueBlock, LongVector timestampVector) {
        var buffer = getBuffer(group);
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
        $Type$Block values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        var sampleCounts = ((LongBlock) page.getBlock(channels.get(2)));
        var resets = (($Type$Block) page.getBlock(channels.get(3)));
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
            state.appendIntervalsFromBlocks(timestamps, values, resets, sampleCounts, valuePosition);
        }
    }

    private void addIntermediateInputBlock(int positionOffset, IntBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        LongBlock timestamps = page.getBlock(channels.get(0));
        $Type$Block values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        var sampleCounts = ((LongBlock) page.getBlock(channels.get(2)));
        var resets = (($Type$Block) page.getBlock(channels.get(3)));
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
                state.appendIntervalsFromBlocks(timestamps, values, resets, sampleCounts, valuePosition);
            }
        }
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (
            var timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
            var values = blockFactory.new$Type$BlockBuilder(positionCount * 2);
            var sampleCounts = blockFactory.newLongVectorFixedBuilder(positionCount);
            var resets = blockFactory.new$Type$VectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushAndCombineState(group);
                // Do not combine intervals across shards because intervals from different indices may overlap.
                if (state != null && state.samples > 0) {
                    timestamps.beginPositionEntry();
                    values.beginPositionEntry();
                    for (Interval interval : state.intervals) {
                        timestamps.appendLong(interval.lastTimestamp);
                        timestamps.appendLong(interval.firstTimestamp);
                        values.append$Type$(interval.lastValue);
                        values.append$Type$(interval.firstValue);
                    }
                    timestamps.endPositionEntry();
                    values.endPositionEntry();
                    sampleCounts.appendLong(state.samples);
                    resets.append$Type$(state.resets);
                } else {
                    timestamps.appendLong(0);
                    values.append$Type$(0);
                    sampleCounts.appendLong(0);
                    resets.append$Type$(0);
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

    private Buffer getBuffer(int groupId) {
        buffers = bigArrays.grow(buffers, groupId + 1);
        Buffer buffer = buffers.get(groupId);
        if (buffer == null) {
            buffer = new Buffer(bigArrays);
            buffers.set(groupId, buffer);
        }
        return buffer;
    }

    private static class Interval implements Comparable<Interval> {

        private long lastTimestamp;
        private $type$ lastValue;
        private long firstTimestamp;
        private $type$ firstValue;
        private $type$ resets;
        private long samples;

        Interval(long timestamp, $type$ value) {
            this(timestamp, value, timestamp, value, 0, 1);
        }

        Interval(long lastTimestamp, $type$ lastValue, long firstTimestamp, $type$ firstValue, $type$ resets, long samples) {
            this.lastTimestamp = lastTimestamp;
            this.lastValue = lastValue;
            this.firstTimestamp = firstTimestamp;
            this.firstValue = firstValue;
            this.resets = resets;
            this.samples = samples;
        }

        boolean maybeAdd(long timestamp, $type$ value) {
            if (timestamp > firstTimestamp) {
                return false;
            }
            if (value > firstValue) {
                resets += value;  // Counter reset detected.
            }
            firstTimestamp = timestamp;
            firstValue = value;
            samples++;
            return true;
        }

        @Override
        public int compareTo(Interval other) {
            return Long.compare(other.lastTimestamp, lastTimestamp); // want most recent first
        }
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
        private final BigArrays bigArrays;
        private ObjectArray<Interval> intervals;
        private int offset = -1;

        Buffer(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            this.intervals = bigArrays.newObjectArray(32);
        }

        void append(long timestamp, $type$ value) {
            if (offset < 0 || intervals.get(offset).maybeAdd(timestamp, value) == false) {
                var nextInterval = new Interval(timestamp, value);
                bigArrays.grow(intervals, offset + 1);
                intervals.set(++offset, nextInterval);
            }
        }

        void appendRange(int fromPosition, int toPosition, $Type$Vector valueVector, LongVector timestampVector) {
            for (int p = fromPosition; p < toPosition; p++) {
                append(timestampVector.getLong(p), valueVector.get$Type$(p));
            }
        }

        void appendRange(int fromPosition, int toPosition, $Type$Block valueBlock, LongVector timestampVector) {
            for (int p = fromPosition; p < toPosition; p++) {
                if (valueBlock.isNull(p)) {
                    continue;
                }
                assert valueBlock.getValueCount(p) == 1 : "expected single-valued block " + valueBlock;
                append(timestampVector.getLong(p), valueBlock.get$Type$(p));
            }
        }

        void flush(ReducedState state) {
            if (offset < 0) {
                return;
            }
            if (offset == 0) {
                state.appendInterval(intervals.get(0));
                return;
            }
            PriorityQueue<Interval> pq = mergeQueue();
            Interval top = pq.pop();

            // first
            final $type$ lastValue = top.lastValue;
            final long lastTimestamp = top.lastTimestamp;
            $type$ resets = top.resets;
            long samples = top.samples;
            $type$ prevValue = top.firstValue;
            while (pq.size() > 0) {
                top = pq.pop();
                samples += top.samples;
                resets += top.resets;
                if (top.lastValue > prevValue) {
                    resets += prevValue;
                }
            }
            state.appendInterval(new Interval(lastTimestamp, lastValue, top.firstTimestamp, top.firstValue, resets, samples));
        }

        private PriorityQueue<Interval> mergeQueue() {
            var pq = new PriorityQueue<Interval>(offset + 1) {
                @Override
                protected boolean lessThan(Interval a, Interval b) {
                    return a.lastTimestamp > b.lastTimestamp; // want the latest timestamp first
                }
            };
            for (int i = 0; i <= offset; i++) {
                pq.add(intervals.get(i));
            }
            return pq;
        }

        @Override
        public void close() {
            intervals.close();
        }
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (
            var rates = blockFactory.newDoubleBlockBuilder(positionCount);
            var flushedStates = new LongObjectPagedHashMap<ReducedState>(positionCount, driverContext.bigArrays())
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushAndCombineState(group);
                if (state != null) {
                    flushedStates.put(group, state);
                    // combine intervals for the final evaluation
                    Interval[] intervals = state.intervals;
                    ArrayUtil.timSort(intervals);
                    for (int i = 1; i < intervals.length; i++) {
                        Interval next = intervals[i - 1]; // reversed
                        Interval prev = intervals[i];
                        if (prev.lastValue > next.firstValue) {
                            state.resets += prev.lastValue;
                        }
                    }
                }
            }
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushedStates.get(group);

                final double rate;
                if (state == null || state.samples == 0) {
                    rate = Double.NaN;
                } else if (evalContext instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                    rate = computeRate(flushedStates, group, tsContext, isRateOverTime, dateFactor);
                } else {
                    rate = computeRateWithoutExtrapolate(state, isRateOverTime, dateFactor);
                }

                if (Double.isNaN(rate)) {
                    rates.appendNull();
                } else {
                    rates.appendDouble(rate);
                }
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
        private static final Interval[] EMPTY_INTERVALS = new Interval[0];
        long samples;
        $type$ resets;
        Interval[] intervals = EMPTY_INTERVALS;

        void appendInterval(Interval interval) {
            int currentSize = intervals.length;
            intervals = ArrayUtil.growExact(intervals, currentSize + 1);
            intervals[currentSize] = interval;
            samples += interval.samples;
            resets += interval.resets;
        }

        void appendIntervalsFromBlocks(LongBlock timestampsBlock, $Type$Block valuesBlock, $Type$Block resetsBlock, LongBlock samplesBLock, int position) {
            int tsFirst = timestampsBlock.getFirstValueIndex(position);
            int vsFirst = valuesBlock.getFirstValueIndex(position);
            int resetsFirst = resetsBlock.getFirstValueIndex(position);
            int samplesFirst = samplesBLock.getFirstValueIndex(position);
            int count = timestampsBlock.getValueCount(position);
            assert count % 2 == 0 : "expected even number of values for intervals, got " + count + " in " + timestampsBlock;
            int currentSize = intervals.length;
            intervals = ArrayUtil.growExact(intervals, currentSize + (count / 2));
            for (int i = 0; i < count; i += 2) {
                Interval interval = new Interval(
                    timestampsBlock.getLong(tsFirst + i),
                    valuesBlock.get$Type$(vsFirst + i),
                    timestampsBlock.getLong(tsFirst + i + 1),
                    valuesBlock.get$Type$(vsFirst + i + 1),
                    resetsBlock.get$Type$(resetsFirst + i),
                    samplesBLock.getLong(samplesFirst + i)
                );
                intervals[currentSize++] = interval;
                samples += interval.samples;
                resets += interval.resets;
            }
        }
    }

    private static double computeRateWithoutExtrapolate(ReducedState state, boolean isRateOverTime, double dateFactor) {
        if (state.samples < 2) {
            return Double.NaN;
        }
        final long firstTS = state.intervals[state.intervals.length - 1].firstTimestamp;
        final long lastTS = state.intervals[0].lastTimestamp;
        double firstValue = state.intervals[state.intervals.length - 1].firstValue;
        double lastValue = state.intervals[0].lastValue + state.resets;
        if (isRateOverTime) {
            return (lastValue - firstValue) * dateFactor / (lastTS - firstTS);
        } else {
            return lastValue - firstValue;
        }
    }

    /**
     * Computes the rate for a given group by interpolating boundary values with adjacent groups,
     * or extrapolating values at the time bucket boundaries.
     */
    private static double computeRate(
        LongObjectPagedHashMap<ReducedState> states,
        int group,
        TimeSeriesGroupingAggregatorEvaluationContext tsContext,
        boolean isRateOverTime,
        double dateFactor
    ) {
        var state = states.get(group);
        final double tbucketStart = tsContext.rangeStartInMillis(group) / 1000.0;
        final double tbucketEnd = tsContext.rangeEndInMillis(group) / 1000.0;
        final double firstValue;
        final double lastValue;
        double firstTsSec = tbucketStart;
        double lastTsSec = tbucketEnd;

        int previousGroupId = tsContext.previousGroupId(group);
        var previousState = (previousGroupId >= 0) ? states.get(previousGroupId) : null;
        if (previousState == null || previousState.samples == 0) {
            if (state.samples == 1) {
                firstTsSec = state.intervals[0].lastTimestamp / dateFactor;
                firstValue = state.intervals[0].lastValue;
            } else {
                firstValue = extrapolateToBoundary(state, tbucketStart, tbucketEnd, dateFactor, true);
            }
        } else {
            firstValue = interpolateBetweenStates(previousState, state, tbucketStart, tbucketEnd, dateFactor, true);
        }

        int nextGroupId = tsContext.nextGroupId(group);
        var nextState = (nextGroupId >= 0) ? states.get(nextGroupId) : null;
        if (nextState == null || nextState.samples == 0) {
            if (state.samples == 1) {
                lastTsSec = state.intervals[0].lastTimestamp / dateFactor;
                lastValue = state.intervals[0].lastValue;
            } else {
                lastValue = extrapolateToBoundary(state, tbucketStart, tbucketEnd, dateFactor, false);
            }
        } else {
            lastValue = interpolateBetweenStates(state, nextState, tbucketStart, tbucketEnd, dateFactor, false) + state.resets;
        }

        if (lastTsSec == firstTsSec) {
            // Check for the case where there is only one sample in state, right at the boundary towards a non-empty adjacent state.
            if (state.samples == 1) {
                if (previousState != null) {
                    assert nextState == null;
                    assert state.intervals[0].lastTimestamp == firstTsSec * dateFactor
                        : firstTsSec + ":" + state.intervals[0].lastTimestamp;
                    final double startTs = previousState.intervals[0].lastTimestamp / dateFactor;
                    final double delta = deltaBetweenStates(previousState, state, dateFactor);
                    return isRateOverTime ? delta / (firstTsSec - startTs) : delta;
                }
                if (nextState != null) {
                    assert state.intervals[0].lastTimestamp == lastTsSec * dateFactor : lastTsSec + ":" + state.intervals[0].lastTimestamp;
                    final double endTs = nextState.intervals[nextState.intervals.length - 1].firstTimestamp / dateFactor;
                    final double delta = deltaBetweenStates(state, nextState, dateFactor);
                    return isRateOverTime ? delta / (endTs - lastTsSec) : delta;
                }
            }
            return Double.NaN;
        }
        final double increase = lastValue - firstValue;
        assert increase >= 0 : "increase must be non-negative, got " + lastValue + " - " + firstValue;
        return (isRateOverTime) ? increase / (lastTsSec - firstTsSec) : increase;
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
    private static double extrapolateToBoundary(
        ReducedState state,
        double tbucketStart,
        double tbucketEnd,
        double dateFactor,
        boolean isLowerBoundary
    ) {
        final double startTs = state.intervals[state.intervals.length - 1].firstTimestamp / dateFactor;
        final double startValue = state.intervals[state.intervals.length - 1].firstValue;
        final double endTs = state.intervals[0].lastTimestamp / dateFactor;
        final double endValue = state.intervals[0].lastValue + state.resets;
        final double sampleTsSec = endTs - startTs;
        final double averageSampleInterval = sampleTsSec / state.samples;
        final double slope = (endValue - startValue) / sampleTsSec;

        if (isLowerBoundary) {
            double startGapSec = startTs - tbucketStart;
            if (startGapSec > 0) {
                if (startGapSec > averageSampleInterval * 1.1) {
                    startGapSec = averageSampleInterval / 2.0;
                }
                return Math.max(0.0, startValue - startGapSec * slope);
            }
            return startValue;
        } else {
            double endGapSec = tbucketEnd - endTs;
            if (endGapSec > 0) {
                if (endGapSec > averageSampleInterval * 1.1) {
                    endGapSec = averageSampleInterval / 2.0;
                }
                return endValue + endGapSec * slope;
            }
            return endValue;
        }
    }

    /**
     * Interpolates the value at the time bucket boundary between two states.
     *
     * For the lower boundary (tbucketStart), interpolation is applied between the last sample of the lower state
     * and the first sample of the upper state. Conversely, for the upper boundary (tbucketEnd), interpolation
     * is applied between the first sample of the lower state and the last sample of the upper state.
     *
     * The logic detects counter resets across the boundary, with interpolation using the last value instead of the
     * value delta to produce correct results.
     */
    private static double interpolateBetweenStates(
        ReducedState lowerState,
        ReducedState upperState,
        double tbucketStart,
        double tbucketEnd,
        double dateFactor,
        boolean isLowerBoundary
    ) {
        final double startValue = lowerState.intervals[0].lastValue;
        final double startTs = lowerState.intervals[0].lastTimestamp / dateFactor;
        final double endValue = upperState.intervals[upperState.intervals.length - 1].firstValue;
        final double endTs = upperState.intervals[upperState.intervals.length - 1].firstTimestamp / dateFactor;
        assert startTs < endTs : "expected startTs < endTs, got " + startTs + " < " + endTs;
        final double delta = deltaBetweenStates(lowerState, upperState, dateFactor);
        final double slope = delta / (endTs - startTs);
        if (isLowerBoundary) {
            assert startTs <= tbucketStart : startTs + " <= " + tbucketStart;
            final double baseValue = (endValue >= startValue) ? startValue : 0;
            double timeDelta = tbucketStart - startTs;
            return baseValue + slope * timeDelta;
        } else {
            assert startTs <= tbucketEnd : startTs + " <= " + tbucketEnd;
            double timeDelta = tbucketEnd - startTs;
            return startValue + slope * timeDelta;
        }
    }

    private static double deltaBetweenStates(ReducedState lowerState, ReducedState upperState, double dateFactor) {
        final double startValue = lowerState.intervals[0].lastValue;
        final double startTs = lowerState.intervals[0].lastTimestamp / dateFactor;
        final double endValue = upperState.intervals[upperState.intervals.length - 1].firstValue;
        final double endTs = upperState.intervals[upperState.intervals.length - 1].firstTimestamp / dateFactor;

        // If the end value is smaller than the start value, a counter reset occurred.
        // In this case, the delta is considered equal to the end value.
        return (endValue >= startValue) ? endValue - startValue : endValue;
    }
}
