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
import org.elasticsearch.compute.data.BytesRefBlock;
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
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogramHolder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;

import java.util.List;

public final class IncreaseExponentialHistogramGroupingAggregatorFunction extends AbstractRateGroupingFunction
    implements
        GroupingAggregatorFunction {
    public static final class FunctionSupplier implements AggregatorFunctionSupplier {
        private final WarningSourceLocation source;

        public FunctionSupplier(WarningSourceLocation source) {
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
        public IncreaseExponentialHistogramGroupingAggregatorFunction groupingAggregator(
            DriverContext driverContext,
            List<Integer> channels
        ) {
            var warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
            return new IncreaseExponentialHistogramGroupingAggregatorFunction(channels, driverContext, warnings);
        }

        @Override
        public String describe() {
            return "increase of ExponentialHistogram";
        }
    }

    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("timestamps", ElementType.LONG),
        new IntermediateStateDesc("values", ElementType.EXPONENTIAL_HISTOGRAM),
        new IntermediateStateDesc("sampleCounts", ElementType.LONG),
        new IntermediateStateDesc("resets", ElementType.EXPONENTIAL_HISTOGRAM)
    );

    private final ExponentialHistogramRawBuffer rawBuffer;
    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    private ObjectArray<ReducedState> reducedStates;
    private final IntervalBuffer intervalBuffer;
    private final Warnings warnings;
    private final ExponentialHistogramMerger.Factory mergerFactory;
    private final ExponentialHistogramCircuitBreaker histoBreaker;
    // track lastSliceIndex to allow flushing the raw buffer when the slice index changed
    private int lastSliceIndex = -1;

    public IncreaseExponentialHistogramGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext, Warnings warnings) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.bigArrays = driverContext.bigArrays();
        this.warnings = warnings;
        this.histoBreaker = new ExponentialHistogramStates.HistoBreaker(driverContext.breaker());
        ExponentialHistogramRawBuffer rawBuffer = null;
        IntervalBuffer intervalBuffer = null;
        ExponentialHistogramMerger.Factory mergerFactory = null;
        try {
            rawBuffer = new ExponentialHistogramRawBuffer(driverContext.blockFactory());
            intervalBuffer = new IntervalBuffer(driverContext.blockFactory());
            mergerFactory = ExponentialHistogramMerger.createFactory(histoBreaker);
            this.reducedStates = bigArrays.newObjectArray(256);
            this.rawBuffer = rawBuffer;
            rawBuffer = null;
            this.intervalBuffer = intervalBuffer;
            intervalBuffer = null;
            this.mergerFactory = mergerFactory;
            mergerFactory = null;
        } finally {
            Releasables.close(rawBuffer, intervalBuffer, mergerFactory);
        }
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        // manage nulls via buffers/reducedStates arrays
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        ExponentialHistogramBlock valuesBlock = page.getBlock(channels.get(0));
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
        TemporalityAccessor temporalityAccessor = TemporalityAccessor.create(temporalityBlock, Temporality.DELTA);
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
                addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector, temporalityAccessor);
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
        ExponentialHistogramBlock valueBlock,
        LongVector timestampVector,
        TemporalityAccessor temporalityAccessor
    ) {
        int lastGroup = -1;
        Temporality temporality = null;
        ReducedState currentDeltaState = null;
        ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
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
                final var value = valueBlock.getExponentialHistogram(valueBlock.getFirstValueIndex(valuePosition), scratch);
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
                        currentDeltaState = getOrInitializeReducedState(groupId);
                        currentDeltaState.appendDeltaValue(timestamp, value);
                    }
                    lastGroup = groupId;
                } else {
                    if (temporality == Temporality.CUMULATIVE) {
                        rawBuffer.maybeResizeAndAppend(timestamp, value);
                    } else if (temporality == Temporality.DELTA) {
                        currentDeltaState.appendDeltaValue(timestamp, value);
                    }
                }
            }
        }
    }

    private void addRawInput(
        int positionOffset,
        IntVector groups,
        ExponentialHistogramBlock valueBlock,
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

    private void addSubRange(
        int group,
        int from,
        int to,
        ExponentialHistogramBlock valueBlock,
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
            ReducedState state = getOrInitializeReducedState(group);
            state.appendDeltaSubRange(from, to, timestampVector, valueBlock);
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
        ExponentialHistogramBlock values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        LongVector sampleCounts = ((LongBlock) page.getBlock(channels.get(2))).asVector();
        ExponentialHistogramBlock resets = page.getBlock(channels.get(3));
        ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            long sampleCount = sampleCounts.getLong(valuePosition);
            if (sampleCount == 0) {
                continue;
            }
            int groupId = groups.getInt(groupPosition);
            ReducedState state = getOrInitializeReducedState(groupId);
            state.appendIntervalsFromBlocks(timestamps, values, valuePosition);
            state.samples += sampleCount;
            var reset = resets.getExponentialHistogram(valuePosition, scratch);
            state.addToResets(reset);
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

    private void addIntermediateInputBlock(int positionOffset, IntBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        LongBlock timestamps = page.getBlock(channels.get(0));
        ExponentialHistogramBlock values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        LongVector sampleCounts = ((LongBlock) page.getBlock(channels.get(2))).asVector();
        ExponentialHistogramBlock resets = page.getBlock(channels.get(3));
        ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
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
                ReducedState state = getOrInitializeReducedState(groupId);
                state.appendIntervalsFromBlocks(timestamps, values, valuePosition);
                state.samples += sampleCount;
                var reset = resets.getExponentialHistogram(valuePosition, scratch);
                state.addToResets(reset);
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
            var values = blockFactory.newExponentialHistogramBlockBuilder(positionCount * 2);
            var sampleCounts = blockFactory.newLongVectorFixedBuilder(positionCount);
            var resets = blockFactory.newExponentialHistogramBlockBuilder(positionCount)
        ) {
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                var state = group < reducedStates.size() ? reducedStates.get(group) : null;
                // Do not combine intervals across shards because intervals from different indices may overlap.
                if (state != null && state.samples > 0) {
                    state.writeIntervalsToBlocks(timestamps, values, scratch);
                    sampleCounts.appendLong(state.samples);
                    resets.append(state.resets == null ? ExponentialHistogram.empty() : state.resets.get());
                } else {
                    timestamps.appendLong(0);
                    values.append(ExponentialHistogram.empty());
                    sampleCounts.appendLong(0);
                    resets.append(ExponentialHistogram.empty());
                }
            }
            blocks[offset] = timestamps.build();
            blocks[offset + 1] = values.build();
            blocks[offset + 2] = sampleCounts.build().asBlock();
            blocks[offset + 3] = resets.build();
        }
    }

    @Override
    public void close() {
        for (int i = 0; i < reducedStates.size(); i++) {
            Releasables.close(reducedStates.get(i));
        }
        Releasables.close(reducedStates, rawBuffer, intervalBuffer, mergerFactory);
    }

    void flushRawBuffers() {
        if (rawBuffer.minGroupId > rawBuffer.maxGroupId) {
            return;
        }
        reducedStates = bigArrays.grow(reducedStates, rawBuffer.maxGroupId + 1);
        var flushQueues = rawBuffer.prepareForFlush();
        for (int groupId = flushQueues.minGroupId(); groupId <= flushQueues.maxGroupId(); groupId++) {
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
        rawBuffer.clearBuffers();
    }

    static final class ExponentialHistogramRawBuffer extends RawBuffer {
        private final ExponentialHistogramBuffer values;

        ExponentialHistogramRawBuffer(BlockFactory factory) {
            super(factory.breaker());
            boolean success = false;
            try {
                this.values = new ExponentialHistogramBuffer(factory, PAGE_SIZE);
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

        void appendWithoutResize(long timestamp, ExponentialHistogram value) {
            timestamps.append(timestamp);
            values.append(value);
        }

        void maybeResizeAndAppend(long timestamp, ExponentialHistogram value) {
            timestamps.ensureCapacity(timestamps.size() + 1);
            values.ensureCapacity(values.size() + 1);
            appendWithoutResize(timestamp, value);
        }

        void appendRange(int fromPosition, int toPosition, ExponentialHistogramBlock valueBlock, LongVector timestampVector) {
            int p = fromPosition;
            while (p < toPosition) {
                while (p < toPosition && valueBlock.isNull(p)) {
                    p++;
                }
                if (p >= toPosition) {
                    break;
                }
                int start = p;
                p++;
                while (p < toPosition && valueBlock.isNull(p) == false) {
                    p++;
                }
                // start to p-1 is now a non-null range
                timestamps.appendRange(timestampVector, start, p - start);
                values.appendRange(valueBlock, start, p - start);
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

    private static class ValueWithScratch {
        ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
        ExponentialHistogram v;

        void load(ExponentialHistogramBuffer buffer, int valueIndex) {
            v = buffer.get(valueIndex, scratch);
        }

        void assignFrom(ValueWithScratch other) {
            // swap the scratches, as the values are just pointers into the scratches
            ExponentialHistogramScratch tmpScratch = scratch;
            scratch = other.scratch;
            other.scratch = tmpScratch;
            v = other.v;
            other.v = null;
        }
    }

    void flushGroup(ReducedState state, ExponentialHistogramRawBuffer buffer, FlushQueue flushQueue) {
        var timestamps = buffer.timestamps;
        var values = buffer.values;
        if (flushQueue.valueCount == 1) {
            state.samples++;
            long t = timestamps.get(flushQueue.top().start);
            var v = values.get(flushQueue.top().start, new ExponentialHistogramScratch());
            state.appendInterval(t, v, t, v);
            return;
        }
        // first
        final long lastTimestamp;
        // JIT scalar replacement should have an easy job on these values
        final ValueWithScratch lastValue = new ValueWithScratch();
        final ValueWithScratch prevValue = new ValueWithScratch();
        Slice top;
        {
            top = flushQueue.top();
            int position = top.next();
            lastTimestamp = timestamps.get(position);
            lastValue.load(values, position);
            prevValue.load(values, position);
            if (top.exhausted()) {
                flushQueue.pop();
                top = flushQueue.top();
            } else {
                top = flushQueue.updateTop();
            }
        }
        final ValueWithScratch currentValue = new ValueWithScratch();
        long secondNextTimestamp = flushQueue.secondNextTimestamp();
        while (flushQueue.size() > 1) {
            // If the last timestamp is greater than the maximum timestamp of the next two candidate slices,
            // there is no overlap with subsequent slices, so batch merging can be performed without comparing
            // timestamps from the buffer.
            if (top.lastTimestamp() > secondNextTimestamp) {
                for (int p = top.start; p < top.end; p++) {
                    currentValue.load(values, p);
                    if (currentValue.v.valueCount() > prevValue.v.valueCount()) {
                        state.addToResets(currentValue.v);
                    }
                    prevValue.assignFrom(currentValue);
                }
                flushQueue.pop();
                top = flushQueue.top();
                secondNextTimestamp = flushQueue.secondNextTimestamp();
                continue;
            }
            currentValue.load(values, top.next());
            if (currentValue.v.valueCount() > prevValue.v.valueCount()) {
                state.addToResets(currentValue.v);
            }
            prevValue.assignFrom(currentValue);
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
            currentValue.load(values, p);
            if (currentValue.v.valueCount() > prevValue.v.valueCount()) {
                state.addToResets(currentValue.v);
            }
            prevValue.assignFrom(currentValue);
        }
        state.samples += flushQueue.valueCount;
        state.appendInterval(lastTimestamp, lastValue.v, timestamps.get(top.end - 1), prevValue.v);
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
        try (
            var rates = blockFactory.newExponentialHistogramBlockBuilder(positionCount);
            var tmp1 = mergerFactory.createMerger();
            var tmp2 = mergerFactory.createMerger()
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                var state = group < reducedStates.size() ? reducedStates.get(group) : null;
                if (state != null && state.samples > 1) {
                    state.combineIntervals();
                }
            }
            if (ctx instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                tsContext.computeAdjacentGroupIds();
            }
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                var state = group < reducedStates.size() ? reducedStates.get(group) : null;

                final ExponentialHistogram rate;
                if (state == null || state.samples == 0) {
                    rate = null;
                } else if (ctx instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                    int previousGroupId = tsContext.previousGroupId(group);
                    var previousState = (0 <= previousGroupId && previousGroupId < reducedStates.size())
                        ? reducedStates.get(previousGroupId)
                        : null;
                    rate = computeIncrease(previousState, state, tmp1, tmp2);
                } else {
                    rate = computeIncrease(null, state, tmp1, tmp2);
                }

                if (rate == null) {
                    rates.appendNull();
                } else {
                    rates.append(rate);
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
        private final ExponentialHistogramBuffer values;

        IntervalBuffer(BlockFactory bf) {
            LongBuffer timestamps = null;
            ExponentialHistogramBuffer values = null;
            boolean success = false;
            try {
                timestamps = new LongBuffer(bf.breaker(), PAGE_SIZE);
                values = new ExponentialHistogramBuffer(bf, PAGE_SIZE);
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

        ExponentialHistogram lastValue(int intervalId, ExponentialHistogramScratch scratch) {
            return values.get(2 * intervalId, scratch);
        }

        long firstTs(int intervalId) {
            return timestamps.get(2 * intervalId + 1);
        }

        ExponentialHistogram firstValue(int intervalId, ExponentialHistogramScratch scratch) {
            return values.get(2 * intervalId + 1, scratch);
        }

        int appendInterval(long lastTs, ExponentialHistogram lastValue, long firstTs, ExponentialHistogram firstValue) {
            int id = count();
            timestamps.ensureCapacity(timestamps.size() + 2);
            values.ensureCapacity(values.size() + 2);
            timestamps.append(lastTs);
            values.append(lastValue);
            timestamps.append(firstTs);
            values.append(firstValue);
            return id;
        }

        int appendIntervalsFromBlocks(LongBlock ts, ExponentialHistogramBlock vs, int position) {
            int tsFirst = ts.getFirstValueIndex(position);
            int vsFirst = vs.getFirstValueIndex(position);
            int valueCount = ts.getValueCount(position);

            assert valueCount % 2 == 0 : "expected even number of values for intervals, got " + valueCount + " in " + ts;

            timestamps.ensureCapacity(timestamps.size() + valueCount);
            values.ensureCapacity(values.size() + valueCount);

            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            int firstId = count();
            for (int i = 0; i < valueCount; i += 2) {
                timestamps.append(ts.getLong(tsFirst + i));
                values.append(vs.getExponentialHistogram(vsFirst + i, scratch));
                timestamps.append(ts.getLong(tsFirst + i + 1));
                values.append(vs.getExponentialHistogram(vsFirst + i + 1, scratch));
            }
            return firstId;
        }

        @Override
        public void close() {
            Releasables.close(timestamps, values);
        }
    }

    final class ReducedState implements Releasable {
        private static final int[] EMPTY_INTERVALS = new int[0];
        long samples;
        ExponentialHistogramMerger resets;

        void addToResets(ExponentialHistogram value) {
            if (value.isEmpty()) {
                return;
            }
            if (resets == null) {
                resets = mergerFactory.createMerger();
            }
            resets.add(value);
        }

        // Points to offsets into IntervalBuffer for the intervals belonging to this group
        // Once sorted (after calling combineIntervals()), the intervals will be stored in reverse chronological order (highest timestamp
        // first)
        int[] intervals = EMPTY_INTERVALS;

        // Delta tracking fields: in contrast to cumulative intervals, they need to be mutable
        // We use deltaLastTs >= deltaFirstTs as indicator delta data exists.
        long deltaFirstTs = Long.MAX_VALUE;
        CompressedExponentialHistogramHolder deltaFirstValue;
        long deltaLastTs = Long.MIN_VALUE;

        boolean hasDelta() {
            return deltaLastTs >= deltaFirstTs;
        }

        void appendInterval(long lastTs, ExponentialHistogram lastValue, long firstTs, ExponentialHistogram firstValue) {
            assert hasDelta() == false : "cannot append intervals while delta data is pending";
            int currentSize = intervals.length;
            this.intervals = ArrayUtil.growExact(intervals, currentSize + 1);
            this.intervals[currentSize] = intervalBuffer.appendInterval(lastTs, lastValue, firstTs, firstValue);
        }

        void appendIntervalsFromBlocks(LongBlock ts, ExponentialHistogramBlock vs, int position) {
            assert hasDelta() == false : "cannot append intervals while delta data is pending";
            int intervalCount = ts.getValueCount(position) / 2;
            int firstIntervalId = intervalBuffer.appendIntervalsFromBlocks(ts, vs, position);
            int currentSize = intervals.length;
            intervals = ArrayUtil.growExact(intervals, currentSize + intervalCount);
            for (int i = 0; i < intervalCount; i++) {
                intervals[currentSize++] = firstIntervalId + i;
            }
        }

        void writeIntervalsToBlocks(
            LongBlock.Builder timestamps,
            ExponentialHistogramBlock.Builder values,
            ExponentialHistogramScratch scratch
        ) {
            timestamps.beginPositionEntry();
            values.beginPositionEntry();
            if (hasDelta()) {
                // delta data gets converted to a single, cumulative interval
                timestamps.appendLong(lastTs());
                timestamps.appendLong(firstTs());
                values.append(lastValue(scratch));
                values.append(firstValue(scratch));
            } else {
                for (int intervalId : intervals) {
                    timestamps.appendLong(intervalBuffer.lastTs(intervalId));
                    timestamps.appendLong(intervalBuffer.firstTs(intervalId));
                    values.append(intervalBuffer.lastValue(intervalId, scratch));
                    values.append(intervalBuffer.firstValue(intervalId, scratch));
                }
            }
            timestamps.endPositionEntry();
            values.endPositionEntry();
        }

        public void appendDeltaValue(long timestamp, ExponentialHistogram value) {
            assert intervals.length == 0 : "cannot append delta data when intervals already exist";
            samples++;
            addToResets(value);
            deltaLastTs = Math.max(deltaLastTs, timestamp);
            if (timestamp < deltaFirstTs) {
                deltaFirstTs = timestamp;
                if (deltaFirstValue == null) {
                    deltaFirstValue = CompressedExponentialHistogramHolder.create(histoBreaker);
                }
                deltaFirstValue.set(value);
            }
        }

        public void appendDeltaSubRange(int from, int to, LongVector timestampVector, ExponentialHistogramBlock valueBlock) {
            assert intervals.length == 0 : "cannot append delta data when intervals already exist";
            int minTsPos = -1;
            long minTs = Long.MAX_VALUE;
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            for (int i = from; i < to; i++) {
                if (valueBlock.isNull(i)) {
                    continue;
                }
                long ts = timestampVector.getLong(i);
                if (ts < minTs) {
                    minTs = ts;
                    minTsPos = i;
                }
                samples++;
                addToResets(valueBlock.getExponentialHistogram(valueBlock.getFirstValueIndex(i), scratch));
                deltaLastTs = Math.max(deltaLastTs, ts);
            }
            if (minTs < deltaFirstTs) {
                deltaFirstTs = minTs;
                if (deltaFirstValue == null) {
                    deltaFirstValue = CompressedExponentialHistogramHolder.create(histoBreaker);
                }
                deltaFirstValue.set(valueBlock.getExponentialHistogram(valueBlock.getFirstValueIndex(minTsPos), scratch));
            }
        }

        void combineIntervals() {
            // only applies to cumulative metrics, we don't need to do anything for delta
            if (hasDelta() == false) {
                // Sort the intervals by the lastTs (most recent first) for the final evaluation
                sortIntervals();
                ExponentialHistogramScratch prevScratch = new ExponentialHistogramScratch();
                ExponentialHistogramScratch nextScratch = new ExponentialHistogramScratch();
                for (int i = 1; i < intervals.length; i++) {
                    int nextInterval = intervals[i - 1]; // reversed
                    int prevInterval = intervals[i];
                    ExponentialHistogram prevLast = intervalBuffer.lastValue(prevInterval, prevScratch);
                    ExponentialHistogram nextFirst = intervalBuffer.firstValue(nextInterval, nextScratch);
                    if (nextFirst.valueCount() < prevLast.valueCount()) {
                        addToResets(prevLast);
                    }
                }
            }
        }

        private void sortIntervals() {
            new IntroSorter() {

                long pivotTs;

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

            }.sort(0, intervals.length);
        }

        // The accessor methods first*/last* must only be called after combineIntervals() for non-delta states!
        long lastTs() {
            if (hasDelta()) {
                return deltaLastTs;
            }
            return intervalBuffer.lastTs(intervals[0]);
        }

        ExponentialHistogram lastValue(ExponentialHistogramScratch scratch) {
            if (hasDelta()) {
                // We use 0 as lastvalue for delta to force resets, the reset counter already has the real value in it
                return ExponentialHistogram.empty();
            }
            return intervalBuffer.lastValue(intervals[0], scratch);
        }

        long firstTs() {
            if (hasDelta()) {
                return deltaFirstTs;
            }
            return intervalBuffer.firstTs(intervals[intervals.length - 1]);
        }

        ExponentialHistogram firstValue(ExponentialHistogramScratch scratch) {
            if (hasDelta()) {
                return deltaFirstValue.accessor();
            }
            return intervalBuffer.firstValue(intervals[intervals.length - 1], scratch);
        }

        @Override
        public void close() {
            Releasables.close(resets, deltaFirstValue);
        }
    }

    private final ExponentialHistogramScratch rateScratchFirst = new ExponentialHistogramScratch();
    private final ExponentialHistogramScratch rateScratchLast = new ExponentialHistogramScratch();
    private final ExponentialHistogramScratch rateScratchPrevLast = new ExponentialHistogramScratch();

    private ExponentialHistogram computeIncrease(
        @Nullable ReducedState previousState,
        ReducedState state,
        ExponentialHistogramMerger tmp1,
        ExponentialHistogramMerger tmp2
    ) {
        if (state.samples == 0) {
            return null;
        }
        long firstTS = state.firstTs();
        final long lastTS = state.lastTs();
        ExponentialHistogram firstValue = state.firstValue(rateScratchFirst);
        ExponentialHistogram lastValue = state.lastValue(rateScratchLast);

        // heuristic, this could be a false-positive if someone decides to ingest empty histograms into their cumulative series
        // but currently there is no metric producing system that does that
        boolean isDelta = lastValue.isEmpty();

        if (previousState != null && previousState.samples > 0) {
            firstTS = previousState.lastTs();
            ExponentialHistogram lastFromPrev = previousState.lastValue(rateScratchPrevLast);
            if (lastFromPrev.valueCount() <= firstValue.valueCount()) {
                firstValue = lastFromPrev;
            } else {
                // use 0 (= EmptyHistogram) in case a reset occurred between the states
                firstValue = ExponentialHistogram.empty();
            }
        } else if (isDelta) {
            // This branch only exists for backwards compatibility with merge_over_time on delta histograms
            // Normally for delta, we would subtract the first sample from the sum of all samples (resets) in the result to be consistent
            // with cumulative
            // however, for backwards compatibility with the old merge_over_time implementation, we don't do this for now
            // we should probably do this in the same way as we do for counters when we add extrapolation / interpolation for histos
            firstTS = -1;
            firstValue = ExponentialHistogram.empty();

        }
        if (firstTS == lastTS) {
            // can't compute increase for cumulative histograms with less than two samples
            return null;
        }
        ExponentialHistogram lastValuePlusRests;
        if (state.resets == null) {
            lastValuePlusRests = lastValue;
        } else if (lastValue.isEmpty()) {
            lastValuePlusRests = state.resets.get();
        } else {
            tmp1.setToMerged(lastValue, state.resets.get());
            lastValuePlusRests = tmp1.get();
        }
        if (firstValue.isEmpty()) {
            return lastValuePlusRests;
        }
        boolean areHistosCumulative = tmp2.setToDifference(lastValuePlusRests, firstValue);
        if (areHistosCumulative == false) {
            warnings.registerException(
                IllegalArgumentException.class,
                "Expected cumulative histograms but they were not, results might be inaccurate"
            );
        }
        return tmp2.get();
    }
}
