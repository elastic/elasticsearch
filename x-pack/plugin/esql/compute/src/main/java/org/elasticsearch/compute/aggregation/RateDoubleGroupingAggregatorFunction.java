/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
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
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;

public final class RateDoubleGroupingAggregatorFunction extends AbstractRateGroupingFunction implements GroupingAggregatorFunction {

    public static final class FunctionSupplier implements AggregatorFunctionSupplier {
        private final boolean isRateOverTime;
        private final boolean isDateNanos;
        private final WarningSourceLocation source;

        public FunctionSupplier(boolean isRateOverTime, boolean isDateNanos, WarningSourceLocation source) {
            this.isRateOverTime = isRateOverTime;
            this.isDateNanos = isDateNanos;
            this.source = source;
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
            var warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
            return new RateDoubleGroupingAggregatorFunction(channels, driverContext, isRateOverTime, isDateNanos, warnings);
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

    // Packed SOA state: STRIDE longs per group at base = groupId * STRIDE.
    // [0] header = (kindBits << KIND_SHIFT) | samples
    // [1] resets = Double.doubleToRawLongBits(cumulativeResets)
    // [2] firstTs (raw long, oldest timestamp)
    // [3] firstValue = Double.doubleToRawLongBits(value at firstTs)
    // [4] lastTs (raw long, most-recent timestamp)
    // [5] lastValue = Double.doubleToRawLongBits(value at lastTs); 0L for DELTA (sentinel)
    //
    // kind values: 0=UNSEEN, 1=DELTA, 2=INLINE (one cumulative interval), 3=SPILLED (multiple intervals)
    private static final int STRIDE = 6;
    private static final int KIND_SHIFT = 62;
    private static final long SAMPLES_MASK = (1L << KIND_SHIFT) - 1;
    private static final long HEADER_DELTA = 1L << KIND_SHIFT;
    private static final long HEADER_INLINE = 2L << KIND_SHIFT;
    private static final long HEADER_SPILLED = 3L << KIND_SHIFT;

    private final DoubleRawBuffer rawBuffer;
    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    private LongArray state;
    private IntervalBuffer intervalBuffer;       // null until first INLINE→SPILLED transition
    private int[][] spilledIntervals;            // null until first spill; [groupId] = int[] of intervalBuffer ids
    private int[] spilledIntervalSizes;          // null until first spill; parallel to spilledIntervals
    private final IntervalSorter intervalSorter = new IntervalSorter();
    private final boolean isRateOverTime;
    private final double dateFactor;
    private final Warnings warnings;

    // track lastSliceIndex to allow flushing the raw buffer when the slice index changed
    private int lastSliceIndex = -1;

    public RateDoubleGroupingAggregatorFunction(
        List<Integer> channels,
        DriverContext driverContext,
        boolean isRateOverTime,
        boolean isDateNanos,
        Warnings warnings
    ) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.isRateOverTime = isRateOverTime;
        this.bigArrays = driverContext.bigArrays();
        this.dateFactor = isDateNanos ? 1_000_000_000.0 : 1000.0;
        this.warnings = warnings;
        DoubleRawBuffer rawBuffer = null;
        try {
            rawBuffer = new DoubleRawBuffer(driverContext.breaker());
            this.state = bigArrays.newLongArray(256L * STRIDE, true);
            this.rawBuffer = rawBuffer;
            rawBuffer = null;
        } finally {
            Releasables.close(rawBuffer);
        }
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        // manage nulls via buffers/state arrays
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
        BytesRefBlock temporalityBlock = page.getBlock(channels.get(2));
        TemporalityAccessor temporalityAccessor = TemporalityAccessor.create(temporalityBlock, Temporality.CUMULATIVE);
        IntVector sliceIndices = ((IntBlock) page.getBlock(channels.get(3))).asVector();
        assert sliceIndices != null : "expected slice indices vector in time-series aggregation";
        LongVector futureMaxTimestamps = ((LongBlock) page.getBlock(channels.get(4))).asVector();
        assert futureMaxTimestamps != null : "expected future max timestamps vector in time-series aggregation";
        int sliceIndex = sliceIndices.getInt(0);
        if (sliceIndex > lastSliceIndex) {
            flushRawBuffers();
            lastSliceIndex = sliceIndex;
        }
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector, temporalityAccessor);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector, temporalityAccessor);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                var valuesVector = valuesBlock.asVector();
                if (valuesVector != null) {
                    addRawInput(positionOffset, groupIds, valuesVector, timestampsVector, temporalityAccessor);
                } else {
                    addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector, temporalityAccessor);
                }
            }

            @Override
            public void close() {

            }
        };
    }

    // Note that this path can be executed randomly in tests, not in production
    private void addRawInput(
        int positionOffset,
        IntBlock groups,
        DoubleBlock valueBlock,
        LongVector timestampVector,
        TemporalityAccessor temporalityAccessor
    ) {
        int lastGroup = -1;
        Temporality temporality = null;
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
                    try {
                        temporality = temporalityAccessor.get(valuePosition);
                    } catch (InvalidTemporalityException e) {
                        warnings.registerException(e);
                        // Set temporality to null to skip all data points in the current group
                        temporality = null;
                    }
                    if (temporality == Temporality.CUMULATIVE) {
                        rawBuffer.prepareForAppend(groupId, 1, timestamp);
                        rawBuffer.appendWithoutResize(timestamp, value);
                    } else if (temporality == Temporality.DELTA) {
                        ensureStateCapacity(groupId);
                        appendDeltaValue(groupId, timestamp, value);
                    }
                    lastGroup = groupId;
                } else {
                    if (temporality == Temporality.CUMULATIVE) {
                        rawBuffer.maybeResizeAndAppend(timestamp, value);
                    } else if (temporality == Temporality.DELTA) {
                        appendDeltaValue(groupId, timestamp, value);
                    }
                }
            }
        }
    }

    private void addRawInput(
        int positionOffset,
        IntVector groups,
        DoubleBlock valueBlock,
        LongVector timestampVector,
        TemporalityAccessor temporalityAccessor
    ) {
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            addSubRange(groupId, positionOffset, positionOffset + positionCount, valueBlock, timestampVector, temporalityAccessor);
        } else {
            int lastGroup = groups.getInt(0);
            int lastPosition = 0;
            for (int p = 1; p < positionCount; p++) {
                int group = groups.getInt(p);
                if (group != lastGroup) {
                    addSubRange(
                        lastGroup,
                        positionOffset + lastPosition,
                        positionOffset + p,
                        valueBlock,
                        timestampVector,
                        temporalityAccessor
                    );
                    lastGroup = group;
                    lastPosition = p;
                }
            }
            addSubRange(
                lastGroup,
                positionOffset + lastPosition,
                positionOffset + positionCount,
                valueBlock,
                timestampVector,
                temporalityAccessor
            );
        }
    }

    private void addRawInput(
        int positionOffset,
        IntVector groups,
        DoubleVector valueVector,
        LongVector timestampVector,
        TemporalityAccessor temporalityAccessor
    ) {
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            addSubRange(groupId, positionOffset, positionOffset + positionCount, valueVector, timestampVector, temporalityAccessor);
        } else {
            int lastGroup = groups.getInt(0);
            int lastPosition = 0;
            for (int p = 1; p < positionCount; p++) {
                int group = groups.getInt(p);
                if (group != lastGroup) {
                    addSubRange(
                        lastGroup,
                        positionOffset + lastPosition,
                        positionOffset + p,
                        valueVector,
                        timestampVector,
                        temporalityAccessor
                    );
                    lastGroup = group;
                    lastPosition = p;
                }
            }
            addSubRange(
                lastGroup,
                positionOffset + lastPosition,
                positionOffset + positionCount,
                valueVector,
                timestampVector,
                temporalityAccessor
            );
        }
    }

    private void addSubRange(
        int group,
        int from,
        int to,
        DoubleVector valueVector,
        LongVector timestampVector,
        TemporalityAccessor temporalityAccessor
    ) {
        final Temporality temporality;
        try {
            temporality = temporalityAccessor.get(from);
        } catch (InvalidTemporalityException e) {
            warnings.registerException(e);
            return;
        }
        if (temporality == Temporality.CUMULATIVE) {
            rawBuffer.prepareForAppend(group, to - from, timestampVector.getLong(from));
            rawBuffer.appendRange(from, to, valueVector, timestampVector);
        } else {
            ensureStateCapacity(group);
            for (int pos = from; pos < to; pos++) {
                appendDeltaValue(group, timestampVector.getLong(pos), valueVector.getDouble(pos));
            }
        }
    }

    private void addSubRange(
        int group,
        int from,
        int to,
        DoubleBlock valueBlock,
        LongVector timestampVector,
        TemporalityAccessor temporalityAccessor
    ) {
        final Temporality temporality;
        try {
            temporality = temporalityAccessor.get(from);
        } catch (InvalidTemporalityException e) {
            warnings.registerException(e);
            return;
        }
        if (temporality == Temporality.CUMULATIVE) {
            rawBuffer.prepareForAppend(group, to - from, timestampVector.getLong(from));
            rawBuffer.appendRange(from, to, valueBlock, timestampVector);
        } else {
            ensureStateCapacity(group);
            for (int pos = from; pos < to; pos++) {
                if (valueBlock.isNull(pos)) {
                    continue;
                }
                assert valueBlock.getValueCount(pos) == 1 : "expected single-valued block " + valueBlock;
                appendDeltaValue(group, timestampVector.getLong(pos), valueBlock.getDouble(valueBlock.getFirstValueIndex(pos)));
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
            ensureStateCapacity(groupId);
            appendIntervalsFromBlocks(groupId, timestamps, values, valuePosition);
            addSamplesAndResets(groupId, sampleCount, resets.getDouble(valuePosition));
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
                ensureStateCapacity(groupId);
                appendIntervalsFromBlocks(groupId, timestamps, values, valuePosition);
                addSamplesAndResets(groupId, sampleCount, resets.getDouble(valuePosition));
            }
        }
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        flushRawBuffers();
        return this::evaluateIntermediate;
    }

    private void evaluateIntermediate(Block[] blocks, int offset, IntVector selectedInPage) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selectedInPage.getPositionCount();
        try (
            var timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
            var values = blockFactory.newDoubleBlockBuilder(positionCount * 2);
            var sampleCounts = blockFactory.newLongVectorFixedBuilder(positionCount);
            var resets = blockFactory.newDoubleVectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                // Do not combine intervals across shards because intervals from different indices may overlap.
                if ((long) group * STRIDE < state.size() && samples(group) > 0) {
                    writeIntervalsToBlocks(group, timestamps, values);
                    sampleCounts.appendLong(samples(group));
                    resets.appendDouble(resets(group));
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
        Releasables.close(state, intervalBuffer, rawBuffer);
    }

    void flushRawBuffers() {
        if (rawBuffer.minGroupId > rawBuffer.maxGroupId) {
            return;
        }
        ensureStateCapacity(rawBuffer.maxGroupId);
        var flushQueues = rawBuffer.prepareForFlush();
        for (int groupId = flushQueues.minGroupId(); groupId <= flushQueues.maxGroupId(); groupId++) {
            var flushQueue = populateReusableFlushQueue(flushQueues, groupId);
            if (flushQueue != null) {
                flushGroup(groupId, rawBuffer, flushQueue);
            }
        }
        rawBuffer.clearBuffers();
    }

    static final class DoubleRawBuffer extends RawBuffer {
        private final DoubleBuffer values;

        DoubleRawBuffer(CircuitBreaker breaker) {
            super(breaker);
            boolean success = false;
            try {
                this.values = new DoubleBuffer(breaker, PAGE_SIZE);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        void prepareForAppend(int groupId, int count, long firstTimestamp) {
            prepareSlicesOnly(groupId, firstTimestamp);
            int newSize = timestamps.size() + count;
            timestamps.ensureCapacity(newSize);
            values.ensureCapacity(newSize);
        }

        void appendWithoutResize(long timestamp, double value) {
            timestamps.append(timestamp);
            values.append(value);
        }

        void maybeResizeAndAppend(long timestamp, double value) {
            timestamps.ensureCapacity(timestamps.size() + 1);
            values.ensureCapacity(values.size() + 1);
            appendWithoutResize(timestamp, value);
        }

        void appendRange(int fromPosition, int toPosition, DoubleVector valueVector, LongVector timestampVector) {
            int count = toPosition - fromPosition;
            timestamps.appendRange(timestampVector, fromPosition, count);
            values.appendRange(valueVector, fromPosition, count);
        }

        void appendRange(int fromPosition, int toPosition, DoubleBlock valueBlock, LongVector timestampVector) {
            for (int p = fromPosition; p < toPosition; p++) {
                if (valueBlock.isNull(p)) {
                    continue;
                }
                assert valueBlock.getValueCount(p) == 1 : "expected single-valued block " + valueBlock;
                timestamps.append(timestampVector.getLong(p));
                values.append(valueBlock.getDouble(p));
            }
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

    void flushGroup(int groupId, DoubleRawBuffer buffer, FlushQueue flushQueue) {
        var timestamps = buffer.timestamps;
        var values = buffer.values;
        if (flushQueue.valueCount == 1) {
            long t = timestamps.get(flushQueue.top().start);
            double v = values.get(flushQueue.top().start);
            appendInterval(groupId, t, v, t, v, 1, 0.0);
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
        double prevValue = lastValue;
        double resetsAccum = 0.0;
        long secondNextTimestamp = flushQueue.secondNextTimestamp();
        while (flushQueue.size() > 1) {
            // If the last timestamp is greater than the maximum timestamp of the next two candidate slices,
            // there is no overlap with subsequent slices, so batch merging can be performed without comparing
            // timestamps from the buffer.
            if (top.lastTimestamp() > secondNextTimestamp) {
                for (int p = top.start; p < top.end; p++) {
                    double val = values.get(p);
                    if (val > prevValue) {
                        resetsAccum += val;
                    }
                    prevValue = val;
                }
                flushQueue.pop();
                top = flushQueue.top();
                secondNextTimestamp = flushQueue.secondNextTimestamp();
                continue;
            }
            double val = values.get(top.next());
            if (val > prevValue) {
                resetsAccum += val;
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
            double val = values.get(p);
            if (val > prevValue) {
                resetsAccum += val;
            }
            prevValue = val;
        }
        appendInterval(groupId, lastTimestamp, lastValue, timestamps.get(top.end - 1), prevValue, flushQueue.valueCount, resetsAccum);
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        flushRawBuffers();
        return (blocks, offset, selectedInPage) -> evaluateFinal(blocks, offset, selectedInPage, ctx);
    }

    private void evaluateFinal(Block[] blocks, int offset, IntVector selectedInPage, GroupingAggregatorEvaluationContext ctx) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selectedInPage.getPositionCount();
        try (var rates = blockFactory.newDoubleBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                if ((long) group * STRIDE < state.size() && samples(group) > 0 && kindBits(group) == 3L) {
                    combineIntervals(group);
                }
            }
            if (ctx instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                tsContext.computeAdjacentGroupIds();
            }
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);

                final double rate;
                if ((long) group * STRIDE >= state.size() || samples(group) == 0) {
                    rate = Double.NaN;
                } else if (ctx instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                    rate = computeRate(group, tsContext, isRateOverTime, dateFactor);
                } else {
                    rate = computeRateWithoutExtrapolate(group, isRateOverTime, dateFactor);
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    static final class IntervalBuffer implements Releasable {

        // Each interval occupies two consecutive slots: slot 2*intervalId stores the last (most recent)
        // timestamp/value pair, slot 2*intervalId+1 stores the first (oldest) timestamp/value pair.
        private final LongBuffer timestamps;
        private final DoubleBuffer values;

        IntervalBuffer(CircuitBreaker cb) {
            LongBuffer timestamps = null;
            DoubleBuffer values = null;
            boolean success = false;
            try {
                timestamps = new LongBuffer(cb, PAGE_SIZE);
                values = new DoubleBuffer(cb, PAGE_SIZE);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(timestamps, values);
                }
            }
            this.timestamps = timestamps;
            this.values = values;
        }

        int count() {
            return timestamps.size() / 2;
        }

        long lastTs(int intervalId) {
            return timestamps.get(2 * intervalId);
        }

        double lastValue(int intervalId) {
            return values.get(2 * intervalId);
        }

        long firstTs(int intervalId) {
            return timestamps.get(2 * intervalId + 1);
        }

        double firstValue(int intervalId) {
            return values.get(2 * intervalId + 1);
        }

        int appendInterval(long lastTs, double lastValue, long firstTs, double firstValue) {
            int id = count();
            timestamps.ensureCapacity(timestamps.size() + 2);
            values.ensureCapacity(values.size() + 2);
            timestamps.append(lastTs);
            values.append(lastValue);
            timestamps.append(firstTs);
            values.append(firstValue);
            return id;
        }

        @Override
        public void close() {
            Releasables.close(timestamps, values);
        }
    }

    // Pre-allocated sorter to avoid anonymous IntroSorter allocation per combineIntervals() call.
    private final class IntervalSorter extends IntroSorter {
        private int[] intervals;
        private int size;
        private long pivotTs;

        void sort(int[] intervals, int size) {
            this.intervals = intervals;
            this.size = size;
            super.sort(0, size);
        }

        @Override
        protected void setPivot(int i) {
            pivotTs = intervalBuffer.lastTs(intervals[i]);
        }

        @Override
        protected int comparePivot(int j) {
            // want most recent first
            return Long.compare(intervalBuffer.lastTs(intervals[j]), pivotTs);
        }

        @Override
        protected int compare(int i, int j) {
            // want most recent first
            return Long.compare(intervalBuffer.lastTs(intervals[j]), intervalBuffer.lastTs(intervals[i]));
        }

        @Override
        protected void swap(int i, int j) {
            int tmp = intervals[i];
            intervals[i] = intervals[j];
            intervals[j] = tmp;
        }
    }

    // ---- SOA state accessors ----

    private void ensureStateCapacity(int groupId) {
        state = bigArrays.grow(state, (long) (groupId + 1) * STRIDE);
    }

    private long kindBits(int groupId) {
        return state.get((long) groupId * STRIDE) >>> KIND_SHIFT;
    }

    private long samples(int groupId) {
        return state.get((long) groupId * STRIDE) & SAMPLES_MASK;
    }

    private double resets(int groupId) {
        return Double.longBitsToDouble(state.get((long) groupId * STRIDE + 1));
    }

    private long firstTs(int groupId) {
        if (kindBits(groupId) == 3L) {
            return intervalBuffer.firstTs(spilledIntervals[groupId][spilledIntervalSizes[groupId] - 1]);
        }
        return state.get((long) groupId * STRIDE + 2);
    }

    private double firstValue(int groupId) {
        if (kindBits(groupId) == 3L) {
            return intervalBuffer.firstValue(spilledIntervals[groupId][spilledIntervalSizes[groupId] - 1]);
        }
        return Double.longBitsToDouble(state.get((long) groupId * STRIDE + 3));
    }

    private long lastTs(int groupId) {
        if (kindBits(groupId) == 3L) {
            return intervalBuffer.lastTs(spilledIntervals[groupId][0]);
        }
        return state.get((long) groupId * STRIDE + 4);
    }

    private double lastValue(int groupId) {
        if (kindBits(groupId) == 3L) {
            return intervalBuffer.lastValue(spilledIntervals[groupId][0]);
        }
        // DELTA: lane 5 is always 0L = 0.0 (sentinel to force reset accounting via resets field)
        // INLINE: stored as Double.doubleToRawLongBits in lane 5
        return Double.longBitsToDouble(state.get((long) groupId * STRIDE + 5));
    }

    private void ensureSpilledArrays(int groupId) {
        if (spilledIntervals == null) {
            spilledIntervals = new int[groupId + 1][];
            spilledIntervalSizes = new int[groupId + 1];
        } else if (groupId >= spilledIntervals.length) {
            int newLen = Math.max(groupId + 1, spilledIntervals.length * 2);
            spilledIntervals = Arrays.copyOf(spilledIntervals, newLen);
            spilledIntervalSizes = Arrays.copyOf(spilledIntervalSizes, newLen);
        }
    }

    private void appendInterval(
        int groupId,
        long lastTs,
        double lastValue,
        long firstTs,
        double firstValue,
        long samplesDelta,
        double resetsDelta
    ) {
        long base = (long) groupId * STRIDE;
        long header = state.get(base);
        long kind = header >>> KIND_SHIFT;
        if (kind == 0L) {
            // UNSEEN → INLINE
            state.set(base, HEADER_INLINE | samplesDelta);
            state.set(base + 1, Double.doubleToRawLongBits(resetsDelta));
            state.set(base + 2, firstTs);
            state.set(base + 3, Double.doubleToRawLongBits(firstValue));
            state.set(base + 4, lastTs);
            state.set(base + 5, Double.doubleToRawLongBits(lastValue));
        } else if (kind == 2L) {
            // INLINE → SPILLED: move existing inline interval into IntervalBuffer, then append new one
            if (intervalBuffer == null) {
                intervalBuffer = new IntervalBuffer(driverContext.breaker());
            }
            ensureSpilledArrays(groupId);
            int oldId = intervalBuffer.appendInterval(
                state.get(base + 4),
                Double.longBitsToDouble(state.get(base + 5)),
                state.get(base + 2),
                Double.longBitsToDouble(state.get(base + 3))
            );
            spilledIntervals[groupId] = new int[] { oldId };
            spilledIntervalSizes[groupId] = 1;
            int newId = intervalBuffer.appendInterval(lastTs, lastValue, firstTs, firstValue);
            spilledIntervals[groupId] = Arrays.copyOf(spilledIntervals[groupId], 2);
            spilledIntervals[groupId][1] = newId;
            spilledIntervalSizes[groupId] = 2;
            long prevSamples = header & SAMPLES_MASK;
            double prevResets = Double.longBitsToDouble(state.get(base + 1));
            state.set(base, HEADER_SPILLED | (prevSamples + samplesDelta));
            state.set(base + 1, Double.doubleToRawLongBits(prevResets + resetsDelta));
        } else {
            // SPILLED: append new interval
            ensureSpilledArrays(groupId);
            int newId = intervalBuffer.appendInterval(lastTs, lastValue, firstTs, firstValue);
            int sz = spilledIntervalSizes[groupId];
            if (sz >= spilledIntervals[groupId].length) {
                spilledIntervals[groupId] = Arrays.copyOf(spilledIntervals[groupId], sz * 2);
            }
            spilledIntervals[groupId][sz] = newId;
            spilledIntervalSizes[groupId] = sz + 1;
            long prevSamples = header & SAMPLES_MASK;
            double prevResets = Double.longBitsToDouble(state.get(base + 1));
            state.set(base, HEADER_SPILLED | (prevSamples + samplesDelta));
            state.set(base + 1, Double.doubleToRawLongBits(prevResets + resetsDelta));
        }
    }

    private void appendDeltaValue(int groupId, long timestamp, double value) {
        assert kindBits(groupId) == 0L || kindBits(groupId) == 1L : "cannot append delta data when intervals already exist";
        long base = (long) groupId * STRIDE;
        long prevHeader = state.get(base);
        long prevSamples = prevHeader & SAMPLES_MASK;
        state.set(base, HEADER_DELTA | (prevSamples + 1));
        // resets field accumulates the raw delta values
        state.set(base + 1, Double.doubleToRawLongBits(Double.longBitsToDouble(state.get(base + 1)) + value));
        if (prevSamples == 0) {
            state.set(base + 2, timestamp);
            state.set(base + 3, Double.doubleToRawLongBits(value));
            state.set(base + 4, timestamp);
            // lane 5 stays 0L = 0.0 (sentinel lastValue for delta)
        } else {
            if (timestamp < state.get(base + 2)) {
                state.set(base + 2, timestamp);
                state.set(base + 3, Double.doubleToRawLongBits(value));
            }
            if (timestamp > state.get(base + 4)) {
                state.set(base + 4, timestamp);
            }
        }
    }

    private void appendIntervalsFromBlocks(int groupId, LongBlock ts, DoubleBlock vs, int position) {
        assert kindBits(groupId) == 0L || kindBits(groupId) == 2L || kindBits(groupId) == 3L
            : "cannot append intervals while delta data is pending";
        int tsFirst = ts.getFirstValueIndex(position);
        int vsFirst = vs.getFirstValueIndex(position);
        int valueCount = ts.getValueCount(position);
        assert valueCount % 2 == 0 : "expected even number of values for intervals, got " + valueCount + " in " + ts;
        for (int i = 0; i < valueCount; i += 2) {
            appendInterval(
                groupId,
                ts.getLong(tsFirst + i),
                vs.getDouble(vsFirst + i),
                ts.getLong(tsFirst + i + 1),
                vs.getDouble(vsFirst + i + 1),
                0,
                0.0
            );
        }
    }

    private void addSamplesAndResets(int groupId, long samplesDelta, double resetsDelta) {
        long base = (long) groupId * STRIDE;
        long prevHeader = state.get(base);
        state.set(base, (prevHeader & ~SAMPLES_MASK) | ((prevHeader & SAMPLES_MASK) + samplesDelta));
        state.set(base + 1, Double.doubleToRawLongBits(Double.longBitsToDouble(state.get(base + 1)) + resetsDelta));
    }

    private void writeIntervalsToBlocks(int groupId, LongBlock.Builder timestamps, DoubleBlock.Builder values) {
        timestamps.beginPositionEntry();
        values.beginPositionEntry();
        long k = kindBits(groupId);
        long base = (long) groupId * STRIDE;
        if (k == 1L) {
            // DELTA: convert to a single cumulative interval; lastValue=0.0 (sentinel forces reset accounting)
            timestamps.appendLong(state.get(base + 4));  // lastTs (max timestamp)
            timestamps.appendLong(state.get(base + 2));  // firstTs (min timestamp)
            values.appendDouble(0.0);                     // lastValue sentinel
            values.appendDouble(Double.longBitsToDouble(state.get(base + 3)));  // firstValue = deltaFirstValue
        } else if (k == 2L) {
            // INLINE: single cumulative interval
            timestamps.appendLong(state.get(base + 4));
            timestamps.appendLong(state.get(base + 2));
            values.appendDouble(Double.longBitsToDouble(state.get(base + 5)));
            values.appendDouble(Double.longBitsToDouble(state.get(base + 3)));
        } else {
            // SPILLED: multiple cumulative intervals
            int sz = spilledIntervalSizes[groupId];
            int[] ivls = spilledIntervals[groupId];
            for (int i = 0; i < sz; i++) {
                int id = ivls[i];
                timestamps.appendLong(intervalBuffer.lastTs(id));
                timestamps.appendLong(intervalBuffer.firstTs(id));
                values.appendDouble(intervalBuffer.lastValue(id));
                values.appendDouble(intervalBuffer.firstValue(id));
            }
        }
        timestamps.endPositionEntry();
        values.endPositionEntry();
    }

    private void combineIntervals(int groupId) {
        // Sort intervals most-recent-first and detect cross-interval counter resets
        int sz = spilledIntervalSizes[groupId];
        int[] ivls = spilledIntervals[groupId];
        intervalSorter.sort(ivls, sz);
        long base = (long) groupId * STRIDE;
        double currentResets = Double.longBitsToDouble(state.get(base + 1));
        for (int i = 1; i < sz; i++) {
            int next = ivls[i - 1]; // most recent (after sort)
            int prev = ivls[i];     // older
            if (intervalBuffer.lastValue(prev) > intervalBuffer.firstValue(next)) {
                currentResets += intervalBuffer.lastValue(prev);
            }
        }
        state.set(base + 1, Double.doubleToRawLongBits(currentResets));
    }

    private double computeRateWithoutExtrapolate(int groupId, boolean isRateOverTime, double dateFactor) {
        if (samples(groupId) < 2) {
            return Double.NaN;
        }
        final long firstTS = firstTs(groupId);
        final long lastTS = lastTs(groupId);
        double firstValue = firstValue(groupId);
        double lastValue = lastValue(groupId) + resets(groupId);
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
        boolean hasPreviousState = previousGroupId >= 0 && (long) previousGroupId * STRIDE < state.size() && samples(previousGroupId) > 0;
        if (hasPreviousState == false) {
            if (samples(group) == 1) {
                firstTsSec = firstTs(group) / dateFactor;
                firstValue = firstValue(group);
            } else {
                firstValue = extrapolateToBoundary(group, tbucketStart, tbucketEnd, dateFactor, true);
            }
        } else {
            firstValue = interpolateBetweenStates(previousGroupId, group, tbucketStart, tbucketEnd, dateFactor, true);
        }

        int nextGroupId = tsContext.nextGroupId(group);
        boolean hasNextState = nextGroupId >= 0 && (long) nextGroupId * STRIDE < state.size() && samples(nextGroupId) > 0;
        if (hasNextState == false) {
            if (samples(group) == 1) {
                lastTsSec = lastTs(group) / dateFactor;
                lastValue = lastValue(group) + resets(group);
            } else {
                lastValue = extrapolateToBoundary(group, tbucketStart, tbucketEnd, dateFactor, false);
            }
        } else {
            lastValue = interpolateBetweenStates(group, nextGroupId, tbucketStart, tbucketEnd, dateFactor, false) + resets(group);
        }

        if (lastTsSec == firstTsSec) {
            // Check for the case where there is only one sample in state, right at the lower boundary
            // of the time bucket towards a non-empty adjacent state.
            // In this case we want to have a result value as the time bucket is not empty,
            // but we already included the increase in the previous time bucket.
            // Therefore, we return the last seen rate of the previous time bucket for rate and zero for increase
            if (samples(group) == 1) {
                if (hasPreviousState) {
                    assert hasNextState == false;
                    assert lastTs(group) == firstTsSec * dateFactor : firstTsSec + ":" + lastTs(group);
                    if (isRateOverTime) {
                        final double startTs = lastTs(previousGroupId) / dateFactor;
                        final double delta = deltaBetweenStates(previousGroupId, group, dateFactor);
                        return delta / (firstTsSec - startTs);
                    } else {
                        return 0.0;
                    }
                }
            }
            return Double.NaN;
        }
        final double increase = lastValue - firstValue;
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
    private double extrapolateToBoundary(int groupId, double tbucketStart, double tbucketEnd, double dateFactor, boolean isLowerBoundary) {
        final double startTs = firstTs(groupId) / dateFactor;
        final double startValue = firstValue(groupId);
        final double endTs = lastTs(groupId) / dateFactor;
        final double endValue = lastValue(groupId) + resets(groupId);
        final double sampleTsSec = endTs - startTs;
        final double averageSampleInterval = sampleTsSec / samples(groupId);
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
    private double interpolateBetweenStates(
        int lowerGroupId,
        int upperGroupId,
        double tbucketStart,
        double tbucketEnd,
        double dateFactor,
        boolean isLowerBoundary
    ) {
        final double startValue = lastValue(lowerGroupId);
        final double startTs = lastTs(lowerGroupId) / dateFactor;
        final double endValue = firstValue(upperGroupId);
        final double endTs = firstTs(upperGroupId) / dateFactor;
        assert startTs < endTs : "expected startTs < endTs, got " + startTs + " < " + endTs;
        final double delta = deltaBetweenStates(lowerGroupId, upperGroupId, dateFactor);
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

    private double deltaBetweenStates(int lowerGroupId, int upperGroupId, double dateFactor) {
        final double startValue = lastValue(lowerGroupId);
        final double endValue = firstValue(upperGroupId);

        // If the end value is smaller than the start value, a counter reset occurred.
        // In this case, the delta is considered equal to the end value.
        return (endValue >= startValue) ? endValue - startValue : endValue;
    }
}
