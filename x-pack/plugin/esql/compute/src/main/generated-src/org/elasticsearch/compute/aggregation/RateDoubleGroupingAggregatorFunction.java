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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.List;
// end generated imports

public final class RateDoubleGroupingAggregatorFunction extends AbstractRateGroupingFunction implements GroupingAggregatorFunction {

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
        public RateDoubleGroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new RateDoubleGroupingAggregatorFunction(channels, driverContext, isRateOverTime, isDateNanos);
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

    private final DoubleRawBuffer rawBuffer;
    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final LocalCircuitBreaker.SingletonService localCircuitBreakerService;
    private final BigArrays bigArrays;
    private ObjectArray<ReducedState> reducedStates;
    private final boolean isRateOverTime;
    private final double dateFactor;

    // track lastSliceIndex to allow flushing the raw buffer when the slice index changed
    private int lastSliceIndex = -1;

    public RateDoubleGroupingAggregatorFunction(
        List<Integer> channels,
        DriverContext driverContext,
        boolean isRateOverTime,
        boolean isDateNanos
    ) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.isRateOverTime = isRateOverTime;
        LocalCircuitBreaker.SingletonService localCircuitBreakerService = new LocalCircuitBreaker.SingletonService(
            driverContext.bigArrays().breakerService(),
            driverContext.localBreakerSettings()
        );
        this.bigArrays = driverContext.bigArrays().withBreakerService(localCircuitBreakerService);
        this.dateFactor = isDateNanos ? 1_000_000_000.0 : 1000.0;
        DoubleRawBuffer buffer = null;
        try {
            buffer = new DoubleRawBuffer(bigArrays);
            this.reducedStates = bigArrays.newObjectArray(256);

            this.rawBuffer = buffer;
            this.localCircuitBreakerService = localCircuitBreakerService;
            buffer = null;
            localCircuitBreakerService = null;
        } finally {
            Releasables.close(buffer, localCircuitBreakerService);
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
        int sliceIndex = sliceIndices.getInt(0);
        if (sliceIndex > lastSliceIndex) {
            flushRawBuffers();
            lastSliceIndex = sliceIndex;
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
    private void addRawInput(int positionOffset, IntBlock groups, DoubleBlock valueBlock, LongVector timestampVector) {
        int lastGroup = -1;
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
                    rawBuffer.prepareForAppend(groupId, 1, timestamp);
                    rawBuffer.appendWithoutResize(timestamp, value);
                    lastGroup = groupId;
                } else {
                    rawBuffer.maybeResizeAndAppend(timestamp, value);
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
        rawBuffer.prepareForAppend(group, to - from, timestampVector.getLong(from));
        rawBuffer.appendRange(from, to, valueVector, timestampVector);
    }

    private void addSubRange(int group, int from, int to, DoubleBlock valueBlock, LongVector timestampVector) {
        rawBuffer.prepareForAppend(group, to - from, timestampVector.getLong(from));
        rawBuffer.appendRange(from, to, valueBlock, timestampVector);
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
            var flushQueues = rawBuffer.prepareForFlush();
            var timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
            var values = blockFactory.newDoubleBlockBuilder(positionCount * 2);
            var sampleCounts = blockFactory.newLongVectorFixedBuilder(positionCount);
            var resets = blockFactory.newDoubleVectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushAndCombineState(flushQueues, group);
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
        Releasables.close(reducedStates, rawBuffer, localCircuitBreakerService);
    }

    void flushRawBuffers() {
        if (rawBuffer.minGroupId > rawBuffer.maxGroupId) {
            return;
        }
        reducedStates = bigArrays.grow(reducedStates, rawBuffer.maxGroupId + 1);
        try (var flushQueues = rawBuffer.prepareForFlush()) {
            for (int groupId = rawBuffer.minGroupId; groupId <= rawBuffer.maxGroupId; groupId++) {
                var flushQueue = flushQueues.getFlushQueue(groupId);
                if (flushQueue != null) {
                    ReducedState state = reducedStates.get(groupId);
                    if (state == null) {
                        state = new ReducedState();
                        reducedStates.set(groupId, state);
                    }
                    flushGroup(state, rawBuffer, flushQueue);
                }
            }
        }
        rawBuffer.minGroupId = Integer.MAX_VALUE;
        rawBuffer.maxGroupId = Integer.MIN_VALUE;
    }

    static final class DoubleRawBuffer extends RawBuffer {
        private DoubleArray values;

        DoubleRawBuffer(BigArrays bigArrays) {
            super(bigArrays);
            boolean success = false;
            try {
                this.values = bigArrays.newDoubleArray(PageCacheRecycler.DOUBLE_PAGE_SIZE, false);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        void prepareForAppend(int groupId, int count, long firstTimestamp) {
            prepareSlicesOnly(groupId, firstTimestamp);
            int newSize = valueCount + count;
            timestamps = bigArrays.grow(timestamps, newSize);
            values = bigArrays.grow(values, newSize);
        }

        void appendWithoutResize(long timestamp, double value) {
            timestamps.set(valueCount, timestamp);
            values.set(valueCount, value);
            valueCount++;
        }

        void maybeResizeAndAppend(long timestamp, double value) {
            timestamps = bigArrays.grow(timestamps, valueCount + 1);
            values = bigArrays.grow(values, valueCount + 1);
            appendWithoutResize(timestamp, value);
        }

        void appendRange(int fromPosition, int toPosition, DoubleVector valueVector, LongVector timestampVector) {
            for (int p = fromPosition; p < toPosition; p++) {
                values.set(valueCount, valueVector.getDouble(p));
                timestamps.set(valueCount, timestampVector.getLong(p));
                valueCount++;
            }
        }

        void appendRange(int fromPosition, int toPosition, DoubleBlock valueBlock, LongVector timestampVector) {
            for (int p = fromPosition; p < toPosition; p++) {
                if (valueBlock.isNull(p)) {
                    continue;
                }
                assert valueBlock.getValueCount(p) == 1 : "expected single-valued block " + valueBlock;
                values.set(valueCount, valueBlock.getDouble(p));
                timestamps.set(valueCount, timestampVector.getLong(p));
                valueCount++;
            }
        }

        @Override
        public void close() {
            Releasables.close(values, super::close);
        }
    }

    static void flushGroup(ReducedState state, DoubleRawBuffer buffer, FlushQueue flushQueue) {
        var timestamps = buffer.timestamps;
        var values = buffer.values;
        if (flushQueue.valueCount == 1) {
            state.samples++;
            long t = timestamps.get(flushQueue.top().start);
            var v = values.get(flushQueue.top().start);
            state.appendInterval(new Interval(t, v, t, v));
            return;
        }
        // first
        final long lastTimestamp;
        final double lastValue;
        Slice top;
        {
            top = flushQueue.top();
            int position = top.next();
            lastTimestamp = timestamps.get(position);
            lastValue = values.get(position);
            if (top.exhausted()) {
                flushQueue.pop();
                top = flushQueue.top();
            } else {
                top = flushQueue.updateTop();
            }
        }
        var prevValue = lastValue;
        long secondNextTimestamp = flushQueue.secondNextTimestamp();
        while (flushQueue.size() > 1) {
            // If the last timestamp is greater than the maximum timestamp of the next two candidate slices,
            // there is no overlap with subsequent slices, so batch merging can be performed without comparing
            // timestamps from the buffer.
            if (top.lastTimestamp() > secondNextTimestamp) {
                for (int p = top.start; p < top.end; p++) {
                    var val = values.get(p);
                    if (val > prevValue) {
                        state.resets += val;
                    }
                    prevValue = val;
                }
                flushQueue.pop();
                top = flushQueue.top();
                secondNextTimestamp = flushQueue.secondNextTimestamp();
                continue;
            }
            var val = values.get(top.next());
            if (val > prevValue) {
                state.resets += val;
            }
            prevValue = val;
            if (top.exhausted()) {
                flushQueue.pop();
                top = flushQueue.top();
                secondNextTimestamp = flushQueue.secondNextTimestamp();
            } else if (top.nextTimestamp < secondNextTimestamp) {
                top = flushQueue.updateTop();
                secondNextTimestamp = flushQueue.secondNextTimestamp();
            }
        }
        // last slice
        top = flushQueue.top();
        for (int p = top.start; p < top.end; p++) {
            var val = values.get(p);
            if (val > prevValue) {
                state.resets += val;
            }
            prevValue = val;
        }
        state.samples += flushQueue.valueCount;
        state.appendInterval(new Interval(lastTimestamp, lastValue, timestamps.get(top.end - 1), prevValue));
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (var flushQueues = rawBuffer.prepareForFlush(); var rates = blockFactory.newDoubleBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = group < reducedStates.size() ? reducedStates.get(group) : null;
                var flushQueue = flushQueues.getFlushQueue(group);
                if (flushQueue != null) {
                    if (state == null) {
                        state = new ReducedState();
                        reducedStates = bigArrays.grow(reducedStates, group + 1);
                        reducedStates.set(group, state);
                    }
                    flushGroup(state, rawBuffer, flushQueue);
                }
                if (state != null && state.samples > 1 && state.intervals.length > 1) {
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
                }
            }
            if (evalContext instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                tsContext.computeAdjacentGroupIds();
            }
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = group < reducedStates.size() ? reducedStates.get(group) : null;

                final double rate;
                if (state == null || state.samples == 0) {
                    rate = Double.NaN;
                } else if (evalContext instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                    rate = computeRate(group, state, tsContext, isRateOverTime, dateFactor);
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

    ReducedState flushAndCombineState(FlushQueues flushQueues, int groupId) {
        ReducedState state = groupId < reducedStates.size() ? reducedStates.getAndSet(groupId, null) : null;
        var flushQueue = flushQueues.getFlushQueue(groupId);
        if (flushQueue != null) {
            if (state == null) {
                state = new ReducedState();
            }
            flushGroup(state, rawBuffer, flushQueue);
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

    private double computeRateWithoutExtrapolate(ReducedState state, boolean isRateOverTime, double dateFactor) {
        if (state.samples < 2) {
            return Double.NaN;
        }
        final long firstTS = state.intervals[state.intervals.length - 1].t2;
        final long lastTS = state.intervals[0].t1;
        double firstValue = state.intervals[state.intervals.length - 1].v2;
        double lastValue = state.intervals[0].v1 + state.resets;
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
    private double computeRate(
        int group,
        ReducedState state,
        TimeSeriesGroupingAggregatorEvaluationContext tsContext,
        boolean isRateOverTime,
        double dateFactor
    ) {
        final double tbucketStart = tsContext.rangeStartInMillis(group) / 1000.0;
        final double tbucketEnd = tsContext.rangeEndInMillis(group) / 1000.0;
        final double firstValue;
        final double lastValue;
        double firstTsSec = tbucketStart;
        double lastTsSec = tbucketEnd;

        int previousGroupId = tsContext.previousGroupId(group);
        var previousState = (0 <= previousGroupId && previousGroupId < reducedStates.size()) ? reducedStates.get(previousGroupId) : null;
        if (previousState == null || previousState.samples == 0) {
            if (state.samples == 1) {
                firstTsSec = state.intervals[0].t1 / dateFactor;
                firstValue = state.intervals[0].v1;
            } else {
                firstValue = extrapolateToBoundary(state, tbucketStart, tbucketEnd, dateFactor, true);
            }
        } else {
            firstValue = interpolateBetweenStates(previousState, state, tbucketStart, tbucketEnd, dateFactor, true);
        }

        int nextGroupId = tsContext.nextGroupId(group);
        var nextState = (nextGroupId >= 0 && nextGroupId < reducedStates.size()) ? reducedStates.get(nextGroupId) : null;
        if (nextState == null || nextState.samples == 0) {
            if (state.samples == 1) {
                lastTsSec = state.intervals[0].t1 / dateFactor;
                lastValue = state.intervals[0].v1;
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
                    assert state.intervals[0].t1 == firstTsSec * dateFactor : firstTsSec + ":" + state.intervals[0].t1;
                    final double startTs = previousState.intervals[0].t1 / dateFactor;
                    final double delta = deltaBetweenStates(previousState, state, dateFactor);
                    return isRateOverTime ? delta / (firstTsSec - startTs) : delta;
                }
                if (nextState != null) {
                    assert state.intervals[0].t1 == lastTsSec * dateFactor : lastTsSec + ":" + state.intervals[0].t1;
                    final double endTs = nextState.intervals[nextState.intervals.length - 1].t2 / dateFactor;
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
        final double startTs = state.intervals[state.intervals.length - 1].t2 / dateFactor;
        final double startValue = state.intervals[state.intervals.length - 1].v2;
        final double endTs = state.intervals[0].t1 / dateFactor;
        final double endValue = state.intervals[0].v1 + state.resets;
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
        final double startValue = lowerState.intervals[0].v1;
        final double startTs = lowerState.intervals[0].t1 / dateFactor;
        final double endValue = upperState.intervals[upperState.intervals.length - 1].v2;
        final double endTs = upperState.intervals[upperState.intervals.length - 1].t2 / dateFactor;
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
        final double startValue = lowerState.intervals[0].v1;
        final double startTs = lowerState.intervals[0].t1 / dateFactor;
        final double endValue = upperState.intervals[upperState.intervals.length - 1].v2;
        final double endTs = upperState.intervals[upperState.intervals.length - 1].t2 / dateFactor;

        // If the end value is smaller than the start value, a counter reset occurred.
        // In this case, the delta is considered equal to the end value.
        return (endValue >= startValue) ? endValue - startValue : endValue;
    }
}
