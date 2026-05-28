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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BreakingExponentialHistogramHolder;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
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
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;

import java.util.List;
// end generated imports

$if(ExponentialHistogram)$
public final class RateExponentialHistogramGroupingAggregatorFunction extends AbstractRateGroupingFunction
    implements
        GroupingAggregatorFunction {
$else$
public final class Rate$Type$GroupingAggregatorFunction extends AbstractRateGroupingFunction implements GroupingAggregatorFunction {

$endif$
    public static final class FunctionSupplier implements AggregatorFunctionSupplier {
        private final boolean isRateOverTime;
        private final boolean isDateNanos;
        private final WarningSourceLocation source;

        public FunctionSupplier(boolean isRateOverTime, boolean isDateNanos, WarningSourceLocation source) {
$if(ExponentialHistogram)$
            if (isRateOverTime) {
                throw new IllegalArgumentException("Only increase is supported for exponential histograms");
            }
$endif$
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
        public Rate$Type$GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            var warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
            return new Rate$Type$GroupingAggregatorFunction(channels, driverContext, isRateOverTime, isDateNanos, warnings);
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
        new IntermediateStateDesc("resets", ElementType.$if(ExponentialHistogram)$EXPONENTIAL_HISTOGRAM$else$DOUBLE$endif$)
    );

    private final $Type$RawBuffer rawBuffer;
    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    private ObjectArray<ReducedState> reducedStates;
    private final IntervalBuffer intervalBuffer;
    private final boolean isRateOverTime;
    private final double dateFactor;
    private final Warnings warnings;
$if(ExponentialHistogram)$
    private final ExponentialHistogramMerger.Factory mergerFactory;
$endif$
    // track lastSliceIndex to allow flushing the raw buffer when the slice index changed
    private int lastSliceIndex = -1;

    public Rate$Type$GroupingAggregatorFunction(
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
        $Type$RawBuffer rawBuffer = null;
        IntervalBuffer intervalBuffer = null;
$if(ExponentialHistogram)$
        ExponentialHistogramMerger.Factory mergerFactory = null;
$endif$
        try {
$if(ExponentialHistogram)$
            rawBuffer = new ExponentialHistogramRawBuffer(driverContext.blockFactory());
            intervalBuffer = new IntervalBuffer(driverContext.blockFactory());
            mergerFactory = ExponentialHistogramMerger.createFactory(new ExponentialHistogramStates.HistoBreaker(driverContext.breaker()));
$else$
            rawBuffer = new $Type$RawBuffer(driverContext.breaker());
            intervalBuffer = new IntervalBuffer(driverContext.breaker());
$endif$
            this.reducedStates = bigArrays.newObjectArray(256);
            this.rawBuffer = rawBuffer;
            rawBuffer = null;
            this.intervalBuffer = intervalBuffer;
            intervalBuffer = null;
$if(ExponentialHistogram)$
            this.mergerFactory = mergerFactory;
            mergerFactory = null;
$endif$
        } finally {
            Releasables.close(rawBuffer, intervalBuffer$if(ExponentialHistogram)$, mergerFactory$endif$);
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
        BytesRefBlock temporalityBlock = page.getBlock(channels.get(2));
$if(ExponentialHistogram)$
        TemporalityAccessor temporalityAccessor = TemporalityAccessor.create(temporalityBlock, Temporality.DELTA);
$else$
        TemporalityAccessor temporalityAccessor = TemporalityAccessor.create(temporalityBlock, Temporality.CUMULATIVE);
$endif$
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
$if(ExponentialHistogram)$
                addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector, temporalityAccessor);
$else$
                var valuesVector = valuesBlock.asVector();
                if (valuesVector != null) {
                    addRawInput(positionOffset, groupIds, valuesVector, timestampsVector, temporalityAccessor);
                } else {
                    addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector, temporalityAccessor);
                }
$endif$
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
        $Type$Block valueBlock,
        LongVector timestampVector,
        TemporalityAccessor temporalityAccessor
    ) {
        int lastGroup = -1;
        Temporality temporality = null;
        ReducedState currentDeltaState = null;
$if(ExponentialHistogram)$
        ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
$endif$
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
                final var value = valueBlock.$if(ExponentialHistogram)$getExponentialHistogram(valueBlock.getFirstValueIndex(valuePosition), scratch)$else$get$Type$(valueBlock.getFirstValueIndex(valuePosition))$endif$;
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
        $Type$Block valueBlock,
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

$if(!ExponentialHistogram)$
    private void addRawInput(
        int positionOffset,
        IntVector groups,
        $Type$Vector valueVector,
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
        $Type$Vector valueVector,
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
            ReducedState state = getOrInitializeReducedState(group);
            state.appendDeltaSubRange(from, to, timestampVector, valueVector);
        }
    }

$endif$
    private void addSubRange(
        int group,
        int from,
        int to,
        $Type$Block valueBlock,
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
        $Type$Block values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        LongVector sampleCounts = ((LongBlock) page.getBlock(channels.get(2))).asVector();
$if(ExponentialHistogram)$
        ExponentialHistogramBlock resets = page.getBlock(channels.get(3));
        ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
$else$
        DoubleVector resets = ((DoubleBlock) page.getBlock(channels.get(3))).asVector();
$endif$
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
$if(ExponentialHistogram)$
            var reset = resets.getExponentialHistogram(valuePosition, scratch);
$else$
            var reset = resets.getDouble(valuePosition);
$endif$
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
        $Type$Block values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        LongVector sampleCounts = ((LongBlock) page.getBlock(channels.get(2))).asVector();
$if(ExponentialHistogram)$
        ExponentialHistogramBlock resets = page.getBlock(channels.get(3));
        ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
$else$
        DoubleVector resets = ((DoubleBlock) page.getBlock(channels.get(3))).asVector();
$endif$
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
$if(ExponentialHistogram)$
                var reset = resets.getExponentialHistogram(valuePosition, scratch);
$else$
                var reset = resets.getDouble(valuePosition);
$endif$
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
            var values = blockFactory.new$Type$BlockBuilder(positionCount * 2);
            var sampleCounts = blockFactory.newLongVectorFixedBuilder(positionCount);
$if(ExponentialHistogram)$
            var resets = blockFactory.newExponentialHistogramBlockBuilder(positionCount)
$else$
            var resets = blockFactory.newDoubleVectorFixedBuilder(positionCount)
$endif$
        ) {
$if(ExponentialHistogram)$
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
$endif$
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                var state = group < reducedStates.size() ? reducedStates.get(group) : null;
                // Do not combine intervals across shards because intervals from different indices may overlap.
                if (state != null && state.samples > 0) {
                    state.writeIntervalsToBlocks(timestamps, values$if(ExponentialHistogram)$, scratch$endif$);
                    sampleCounts.appendLong(state.samples);
$if(ExponentialHistogram)$
                    resets.append(state.resets == null ? ExponentialHistogram.empty() : state.resets.get());
$else$
                    resets.appendDouble(state.resets);
$endif$
                } else {
                    timestamps.appendLong(0);
$if(ExponentialHistogram)$
                    values.append(ExponentialHistogram.empty());
                    sampleCounts.appendLong(0);
                    resets.append(ExponentialHistogram.empty());
$else$
                    values.append$Type$(0);
                    sampleCounts.appendLong(0);
                    resets.appendDouble(0);
$endif$
                }
            }
            blocks[offset] = timestamps.build();
            blocks[offset + 1] = values.build();
            blocks[offset + 2] = sampleCounts.build().asBlock();
$if(ExponentialHistogram)$
            blocks[offset + 3] = resets.build();
$else$
            blocks[offset + 3] = resets.build().asBlock();
$endif$
        }
    }

    @Override
    public void close() {
        for (int i = 0; i < reducedStates.size(); i++) {
            Releasables.close(reducedStates.get(i));
        }
        Releasables.close(reducedStates, rawBuffer, intervalBuffer$if(ExponentialHistogram)$, mergerFactory$endif$);
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

    static final class $Type$RawBuffer extends RawBuffer {
        private final $Type$Buffer values;

$if(ExponentialHistogram)$
        ExponentialHistogramRawBuffer(BlockFactory factory) {
            super(factory.breaker());
            boolean success = false;
            try {
                this.values = new ExponentialHistogramBuffer(factory, PAGE_SIZE);
$else$
        $Type$RawBuffer(CircuitBreaker breaker) {
            super(breaker);
            boolean success = false;
            try {
                this.values = new $Type$Buffer(breaker, PAGE_SIZE);
$endif$
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

        void appendWithoutResize(long timestamp, $type$ value) {
            timestamps.append(timestamp);
            values.append(value);
        }

        void maybeResizeAndAppend(long timestamp, $type$ value) {
            timestamps.ensureCapacity(timestamps.size() + 1);
            values.ensureCapacity(values.size() + 1);
            appendWithoutResize(timestamp, value);
        }

$if(!ExponentialHistogram)$
        void appendRange(int fromPosition, int toPosition, $Type$Vector valueVector, LongVector timestampVector) {
            int count = toPosition - fromPosition;
            timestamps.appendRange(timestampVector, fromPosition, count);
            values.appendRange(valueVector, fromPosition, count);
        }

$endif$
        void appendRange(int fromPosition, int toPosition, $Type$Block valueBlock, LongVector timestampVector) {
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
$if(ExponentialHistogram)$
                values.appendRange(valueBlock, start, p - start);
$else$
                for (int i = start; i < p; i++) {
                    assert valueBlock.getValueCount(i) == 1 : "expected single-valued block " + valueBlock;
                    values.append(valueBlock.get$Type$(valueBlock.getFirstValueIndex(i)));
                }
$endif$
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

    static boolean isCumulativeReset($type$ value, $type$ valueAfter) {
$if(ExponentialHistogram)$
        return valueAfter.valueCount() < value.valueCount();
$else$
        return valueAfter < value;
$endif$
    }

    private static class ValueWithTimestamp {
$if(ExponentialHistogram)$
        ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
$endif$
        $type$ v;
        long ts;

        void loadValue($Type$RawBuffer buffer, int valueIndex) {
            ts = buffer.timestamps.get(valueIndex);
            v = buffer.values.get(valueIndex$if(ExponentialHistogram)$, scratch$endif$);
        }

        void swapWith(ValueWithTimestamp other) {
$if(ExponentialHistogram)$
            ExponentialHistogramScratch tmpScratch = scratch;
            scratch = other.scratch;
            other.scratch = tmpScratch;
$endif$
            $type$ tmpV = v;
            v = other.v;
            other.v = tmpV;

            long tmpTs = ts;
            ts = other.ts;
            other.ts = tmpTs;
        }
    }

    void flushGroup(ReducedState state, $Type$RawBuffer buffer, FlushQueue flushQueue) {
        if (flushQueue.valueCount == 1) {
            state.samples++;
            final var first = new ValueWithTimestamp();
            first.loadValue(buffer, flushQueue.top().start);
            state.appendInterval(first.ts, first.v, first.ts, first.v);
            return;
        }
        // JIT scalar replacement should have an easy job on these three values below
        // making this as fast as directly using primitives
        final var prev = new ValueWithTimestamp();
        final var current = new ValueWithTimestamp();
        final var last = new ValueWithTimestamp();
        // first in queue, so last in time (most recent)
        Slice top;
        {
            top = flushQueue.top();
            int position = top.next();
            last.loadValue(buffer, position);
            prev.loadValue(buffer, position);
            if (top.exhausted()) {
                flushQueue.pop();
                top = flushQueue.top();
            } else {
                top = flushQueue.updateTop();
            }
        }
        long secondNextTimestamp = flushQueue.secondNextTimestamp();
        while (flushQueue.size() > 1) {
            // If the last timestamp is greater than the maximum timestamp of the next two candidate slices,
            // there is no overlap with subsequent slices, so batch merging can be performed without comparing
            // timestamps from the buffer.
            if (top.lastTimestamp() > secondNextTimestamp) {
                for (int p = top.start; p < top.end; p++) {
                    current.loadValue(buffer, p);
                    // prev is more recent (later in time) than current, hence the order
                    if (isCumulativeReset(current.v, prev.v)) {
                        state.addToResets(current.v);
                    }
                    prev.swapWith(current);
                }
                flushQueue.pop();
                top = flushQueue.top();
                secondNextTimestamp = flushQueue.secondNextTimestamp();
                continue;
            }
            current.loadValue(buffer, top.next());
            // prev is more recent (later in time) than current, hence the order
            if (isCumulativeReset(current.v, prev.v)) {
                state.addToResets(current.v);
            }
            prev.swapWith(current);
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
            current.loadValue(buffer, p);
            // prev is more recent (later in time) than current, hence the order
            if (isCumulativeReset(current.v, prev.v)) {
                state.addToResets(current.v);
            }
            prev.swapWith(current);
        }
        state.samples += flushQueue.valueCount;
        state.appendInterval(last.ts, last.v, buffer.timestamps.get(top.end - 1), prev.v);
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
$if(ExponentialHistogram)$
        try (
            var rates = blockFactory.newExponentialHistogramBlockBuilder(positionCount);
            var tmp1 = mergerFactory.createMerger();
            var tmp2 = mergerFactory.createMerger()
        ) {
$else$
        try (var rates = blockFactory.newDoubleBlockBuilder(positionCount)) {
$endif$
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

$if(ExponentialHistogram)$
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
$else$
                final double rate;
                if (state == null || state.samples == 0) {
                    rate = Double.NaN;
                } else if (ctx instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                    rate = computeRate(group, state, tsContext, isRateOverTime, dateFactor);
                } else {
                    rate = computeRateWithoutExtrapolate(state, isRateOverTime, dateFactor);
                }

                if (Double.isNaN(rate)) {
                    rates.appendNull();
                } else {
                    rates.appendDouble(rate);
                }
$endif$
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
        private final $Type$Buffer values;

$if(ExponentialHistogram)$
        IntervalBuffer(BlockFactory bf) {
            LongBuffer timestamps = null;
            ExponentialHistogramBuffer values = null;
            boolean success = false;
            try {
                timestamps = new LongBuffer(bf.breaker(), PAGE_SIZE);
                values = new ExponentialHistogramBuffer(bf, PAGE_SIZE);
$else$
        IntervalBuffer(CircuitBreaker cb) {
            LongBuffer timestamps = null;
            $Type$Buffer values = null;
            boolean success = false;
            try {
                timestamps = new LongBuffer(cb, PAGE_SIZE);
                values = new $Type$Buffer(cb, PAGE_SIZE);
$endif$
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

        $type$ lastValue(int intervalId$if(ExponentialHistogram)$, ExponentialHistogramScratch scratch$endif$) {
            return values.get(2 * intervalId$if(ExponentialHistogram)$, scratch$endif$);
        }

        long firstTs(int intervalId) {
            return timestamps.get(2 * intervalId + 1);
        }

        $type$ firstValue(int intervalId$if(ExponentialHistogram)$, ExponentialHistogramScratch scratch$endif$) {
            return values.get(2 * intervalId + 1$if(ExponentialHistogram)$, scratch$endif$);
        }

        int appendInterval(long lastTs, $type$ lastValue, long firstTs, $type$ firstValue) {
            int id = count();
            timestamps.ensureCapacity(timestamps.size() + 2);
            values.ensureCapacity(values.size() + 2);
            timestamps.append(lastTs);
            values.append(lastValue);
            timestamps.append(firstTs);
            values.append(firstValue);
            return id;
        }

        int appendIntervalsFromBlocks(LongBlock ts, $Type$Block vs, int position) {
            int tsFirst = ts.getFirstValueIndex(position);
            int vsFirst = vs.getFirstValueIndex(position);
            int valueCount = ts.getValueCount(position);

            assert valueCount % 2 == 0 : "expected even number of values for intervals, got " + valueCount + " in " + ts;

            timestamps.ensureCapacity(timestamps.size() + valueCount);
            values.ensureCapacity(values.size() + valueCount);

$if(ExponentialHistogram)$
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
$endif$
            int firstId = count();
            for (int i = 0; i < valueCount; i += 2) {
                timestamps.append(ts.getLong(tsFirst + i));
                values.append(vs.get$Type$(vsFirst + i$if(ExponentialHistogram)$, scratch$endif$));
                timestamps.append(ts.getLong(tsFirst + i + 1));
                values.append(vs.get$Type$(vsFirst + i + 1$if(ExponentialHistogram)$, scratch$endif$));
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
$if(ExponentialHistogram)$
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

$else$
        double resets;

        void addToResets(double value) {
            resets += value;
        }
$endif$

        // Points to offsets into IntervalBuffer for the intervals belonging to this group
        // Once sorted (after calling combineIntervals()), the intervals will be stored in reverse chronological order (highest timestamp
        // first)
        int[] intervals = EMPTY_INTERVALS;

        // Delta tracking fields: in contrast to cumulative intervals, they need to be mutable
        // We use deltaLastTs >= deltaFirstTs as indicator delta data exists.
        long deltaFirstTs = Long.MAX_VALUE;
$if(ExponentialHistogram)$
        BreakingExponentialHistogramHolder deltaFirstValue;
$else$
        $type$ deltaFirstValue;
$endif$
        long deltaLastTs = Long.MIN_VALUE;

        boolean hasDelta() {
            return deltaLastTs >= deltaFirstTs;
        }

        void appendInterval(long lastTs, $type$ lastValue, long firstTs, $type$ firstValue) {
            assert hasDelta() == false : "cannot append intervals while delta data is pending";
            int currentSize = intervals.length;
            this.intervals = ArrayUtil.growExact(intervals, currentSize + 1);
            this.intervals[currentSize] = intervalBuffer.appendInterval(lastTs, lastValue, firstTs, firstValue);
        }

        void appendIntervalsFromBlocks(LongBlock ts, $Type$Block vs, int position) {
            assert hasDelta() == false : "cannot append intervals while delta data is pending";
            int intervalCount = ts.getValueCount(position) / 2;
            int firstIntervalId = intervalBuffer.appendIntervalsFromBlocks(ts, vs, position);
            int currentSize = intervals.length;
            intervals = ArrayUtil.growExact(intervals, currentSize + intervalCount);
            for (int i = 0; i < intervalCount; i++) {
                intervals[currentSize++] = firstIntervalId + i;
            }
        }

$if(ExponentialHistogram)$
        void writeIntervalsToBlocks(
            LongBlock.Builder timestamps,
            ExponentialHistogramBlock.Builder values,
            ExponentialHistogramScratch scratch
        ) {
$else$
        void writeIntervalsToBlocks(LongBlock.Builder timestamps, $Type$Block.Builder values) {
$endif$
            timestamps.beginPositionEntry();
            values.beginPositionEntry();
            if (hasDelta()) {
                // delta data gets converted to a single, cumulative interval
                timestamps.appendLong(lastTs());
                timestamps.appendLong(firstTs());
$if(ExponentialHistogram)$
                values.append(lastValue(scratch));
                values.append(firstValue(scratch));
$else$
                values.append$Type$(lastValue());
                values.append$Type$(firstValue());
$endif$
            } else {
                for (int intervalId : intervals) {
                    timestamps.appendLong(intervalBuffer.lastTs(intervalId));
                    timestamps.appendLong(intervalBuffer.firstTs(intervalId));
$if(ExponentialHistogram)$
                    values.append(intervalBuffer.lastValue(intervalId, scratch));
                    values.append(intervalBuffer.firstValue(intervalId, scratch));
$else$
                    values.append$Type$(intervalBuffer.lastValue(intervalId));
                    values.append$Type$(intervalBuffer.firstValue(intervalId));
$endif$
                }
            }
            timestamps.endPositionEntry();
            values.endPositionEntry();
        }

        public void appendDeltaValue(long timestamp, $type$ value) {
            assert intervals.length == 0 : "cannot append delta data when intervals already exist";
            samples++;
            addToResets(value);
            deltaLastTs = Math.max(deltaLastTs, timestamp);
            if (timestamp < deltaFirstTs) {
                deltaFirstTs = timestamp;
$if(ExponentialHistogram)$
                if (deltaFirstValue == null) {
                    deltaFirstValue = BreakingExponentialHistogramHolder.create(driverContext.breaker());
                }
                deltaFirstValue.set(value);
$else$
                deltaFirstValue = value;
$endif$
            }
        }

        public void appendDeltaSubRange(int from, int to, LongVector timestampVector, $Type$Block valueBlock) {
            assert intervals.length == 0 : "cannot append delta data when intervals already exist";
            int minTsPos = -1;
            long minTs = Long.MAX_VALUE;
$if(ExponentialHistogram)$
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
$endif$
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
                addToResets(valueBlock.get$Type$(valueBlock.getFirstValueIndex(i)$if(ExponentialHistogram)$, scratch$endif$));
                deltaLastTs = Math.max(deltaLastTs, ts);
            }
            if (minTs < deltaFirstTs) {
                deltaFirstTs = minTs;
$if(ExponentialHistogram)$
                if (deltaFirstValue == null) {
                    deltaFirstValue = BreakingExponentialHistogramHolder.create(driverContext.breaker());
                }
                deltaFirstValue.set(valueBlock.getExponentialHistogram(valueBlock.getFirstValueIndex(minTsPos), scratch));
$else$
                deltaFirstValue = valueBlock.get$Type$(valueBlock.getFirstValueIndex(minTsPos));
$endif$
            }
        }

$if(!ExponentialHistogram)$
        public void appendDeltaSubRange(int from, int to, LongVector timestampVector, $Type$Vector valueVector) {
            assert intervals.length == 0 : "cannot append delta data when intervals already exist";
            int minTsPos = -1;
            long minTs = Long.MAX_VALUE;
            for (int i = from; i < to; i++) {
                long ts = timestampVector.getLong(i);
                if (ts < minTs) {
                    minTs = ts;
                    minTsPos = i;
                }
                samples++;
                addToResets(valueVector.get$Type$(i));
                deltaLastTs = Math.max(deltaLastTs, ts);
            }
            if (minTs < deltaFirstTs) {
                deltaFirstTs = minTs;
                deltaFirstValue = valueVector.get$Type$(minTsPos);
            }
        }

$endif$
        void combineIntervals() {
            // only applies to cumulative metrics, we don't need to do anything for delta
            if (hasDelta() == false) {
                // Sort the intervals by the lastTs (most recent first) for the final evaluation
                sortIntervals();
$if(ExponentialHistogram)$
                ExponentialHistogramScratch prevScratch = new ExponentialHistogramScratch();
                ExponentialHistogramScratch nextScratch = new ExponentialHistogramScratch();
$endif$
                for (int i = 1; i < intervals.length; i++) {
                    int nextInterval = intervals[i - 1]; // reversed
                    int prevInterval = intervals[i];
                    $type$ prevLast = intervalBuffer.lastValue(prevInterval$if(ExponentialHistogram)$, prevScratch$endif$);
                    $type$ nextFirst = intervalBuffer.firstValue(nextInterval$if(ExponentialHistogram)$, nextScratch$endif$);
                    if (isCumulativeReset(prevLast, nextFirst)) {
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

        $type$ lastValue($if(ExponentialHistogram)$ExponentialHistogramScratch scratch$endif$) {
            if (hasDelta()) {
                // We use 0 as lastvalue for delta to force resets, the reset counter already has the real value in it
$if(ExponentialHistogram)$
                return ExponentialHistogram.empty();
$else$
                return 0;
$endif$
            }
            return intervalBuffer.lastValue(intervals[0]$if(ExponentialHistogram)$, scratch$endif$);
        }

        long firstTs() {
            if (hasDelta()) {
                return deltaFirstTs;
            }
            return intervalBuffer.firstTs(intervals[intervals.length - 1]);
        }

        $type$ firstValue($if(ExponentialHistogram)$ExponentialHistogramScratch scratch$endif$) {
            if (hasDelta()) {
                return deltaFirstValue$if(ExponentialHistogram)$.accessor()$endif$;
            }
            return intervalBuffer.firstValue(intervals[intervals.length - 1]$if(ExponentialHistogram)$, scratch$endif$);
        }

        @Override
        public void close() {
$if(ExponentialHistogram)$
            Releasables.close(resets, deltaFirstValue);
$else$
            // no-op
$endif$
        }
    }

$if(ExponentialHistogram)$
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
$else$
    private double computeRateWithoutExtrapolate(ReducedState state, boolean isRateOverTime, double dateFactor) {
        if (state.samples < 2) {
            return Double.NaN;
        }
        final long firstTS = state.firstTs();
        final long lastTS = state.lastTs();
        double firstValue = state.firstValue();
        double lastValue = state.lastValue() + state.resets;
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
                firstTsSec = state.firstTs() / dateFactor;
                firstValue = state.firstValue();
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
                lastTsSec = state.lastTs() / dateFactor;
                lastValue = state.lastValue() + state.resets;
            } else {
                lastValue = extrapolateToBoundary(state, tbucketStart, tbucketEnd, dateFactor, false);
            }
        } else {
            lastValue = interpolateBetweenStates(state, nextState, tbucketStart, tbucketEnd, dateFactor, false) + state.resets;
        }

        if (lastTsSec == firstTsSec) {
            // Check for the case where there is only one sample in state, right at the lower boundary
            // of the time bucket towards a non-empty adjacent state.
            // In this case we want to have a result value as the time bucket is not empty,
            // but we already included the increase in the previous time bucket.
            // Therefore, we return the last seen rate of the previous time bucket for rate and zero for increase
            if (state.samples == 1) {
                if (previousState != null) {
                    assert nextState == null;
                    assert state.lastTs() == firstTsSec * dateFactor : firstTsSec + ":" + state.lastTs();
                    if (isRateOverTime) {
                        final double startTs = previousState.lastTs() / dateFactor;
                        final double delta = deltaBetweenStates(previousState, state, dateFactor);
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
    private double extrapolateToBoundary(
        ReducedState state,
        double tbucketStart,
        double tbucketEnd,
        double dateFactor,
        boolean isLowerBoundary
    ) {
        final double startTs = state.firstTs() / dateFactor;
        final double startValue = state.firstValue();
        final double endTs = state.lastTs() / dateFactor;
        final double endValue = state.lastValue() + state.resets;
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
    private double interpolateBetweenStates(
        ReducedState lowerState,
        ReducedState upperState,
        double tbucketStart,
        double tbucketEnd,
        double dateFactor,
        boolean isLowerBoundary
    ) {
        final double startValue = lowerState.lastValue();
        final double startTs = lowerState.lastTs() / dateFactor;
        final double endValue = upperState.firstValue();
        final double endTs = upperState.firstTs() / dateFactor;
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

    private double deltaBetweenStates(ReducedState lowerState, ReducedState upperState, double dateFactor) {
        final double startValue = lowerState.lastValue();
        final double endValue = upperState.firstValue();

        // If the end value is smaller than the start value, a counter reset occurred.
        // In this case, the delta is considered equal to the end value.
        return (endValue >= startValue) ? endValue - startValue : endValue;
    }
$endif$
}
