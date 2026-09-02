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
        new IntermediateStateDesc("changes", ElementType.LONG)
    );

    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    private final IntRawBuffer rawBuffer;
    private final PointBuffer pointBuffer;
    private final IntervalBuffer intervalBuffer;
    private ObjectArray<ReducedState> states;

    ChangesIntGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.bigArrays = driverContext.bigArrays();
        IntRawBuffer rawBuffer = null;
        PointBuffer pointBuffer = null;
        IntervalBuffer intervalBuffer = null;
        try {
            rawBuffer = new IntRawBuffer(driverContext.breaker());
            pointBuffer = new PointBuffer(driverContext.breaker());
            intervalBuffer = new IntervalBuffer(driverContext.breaker());
            this.states = bigArrays.newObjectArray(256);
            this.rawBuffer = rawBuffer;
            rawBuffer = null;
            this.pointBuffer = pointBuffer;
            pointBuffer = null;
            this.intervalBuffer = intervalBuffer;
            intervalBuffer = null;
        } finally {
            Releasables.close(rawBuffer, pointBuffer, intervalBuffer);
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
        IntBlock valuesBlock = page.getBlock(channels.get(0));
        if (valuesBlock.areAllValuesNull()) {
            return null;
        }
        LongBlock timestampsBlock = page.getBlock(channels.get(1));
        if (timestampsBlock.areAllValuesNull()) {
            return null;
        }
        IntVector valuesVector = valuesBlock.asVector();
        LongVector timestampsVector = timestampsBlock.asVector();
        if (valuesVector != null && timestampsVector != null) {
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
                public void close() {
                    flushRawBuffers();
                }
            };
        }
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groups) {
                addRawInput(positionOffset, groups, valuesBlock, timestampsBlock);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groups) {
                addRawInput(positionOffset, groups, valuesBlock, timestampsBlock);
            }

            @Override
            public void add(int positionOffset, IntVector groups) {
                addRawInput(positionOffset, groups, valuesBlock, timestampsBlock);
            }

            @Override
            public void close() {
                flushRawBuffers();
            }
        };
    }

    private void addRawInput(int positionOffset, IntBlock groups, IntBlock valuesBlock, LongBlock timestampsBlock) {
        int positionCount = groups.getPositionCount();
        for (int groupPosition = 0; groupPosition < positionCount; groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuePosition = groupPosition + positionOffset;
            if (valuesBlock.isNull(valuePosition) || timestampsBlock.isNull(valuePosition)) {
                continue;
            }
            assert valuesBlock.getValueCount(valuePosition) == 1 : "expected single-valued block " + valuesBlock;
            assert timestampsBlock.getValueCount(valuePosition) == 1 : "expected single-valued block " + timestampsBlock;
            int value = valuesBlock.getInt(valuesBlock.getFirstValueIndex(valuePosition));
            long timestamp = timestampsBlock.getLong(timestampsBlock.getFirstValueIndex(valuePosition));
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                appendRaw(groups.getInt(g), timestamp, value);
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, IntBlock valuesBlock, LongBlock timestampsBlock) {
        int positionCount = groups.getPositionCount();
        for (int groupPosition = 0; groupPosition < positionCount; groupPosition++) {
            int valuePosition = groupPosition + positionOffset;
            if (valuesBlock.isNull(valuePosition) || timestampsBlock.isNull(valuePosition)) {
                continue;
            }
            assert valuesBlock.getValueCount(valuePosition) == 1 : "expected single-valued block " + valuesBlock;
            assert timestampsBlock.getValueCount(valuePosition) == 1 : "expected single-valued block " + timestampsBlock;
            appendRaw(
                groups.getInt(groupPosition),
                timestampsBlock.getLong(timestampsBlock.getFirstValueIndex(valuePosition)),
                valuesBlock.getInt(valuesBlock.getFirstValueIndex(valuePosition))
            );
        }
    }

    private void addRawInput(int positionOffset, IntBlock groups, IntVector valuesVector, LongVector timestampsVector) {
        int positionCount = groups.getPositionCount();
        for (int groupPosition = 0; groupPosition < positionCount; groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuePosition = groupPosition + positionOffset;
            int value = valuesVector.getInt(valuePosition);
            long timestamp = timestampsVector.getLong(valuePosition);
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                appendRaw(groups.getInt(g), timestamp, value);
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, IntVector valuesVector, LongVector timestampsVector) {
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            rawBuffer.prepareForAppend(groupId, positionCount, timestampsVector.getLong(positionOffset));
            rawBuffer.appendRange(positionOffset, positionOffset + positionCount, valuesVector, timestampsVector);
            return;
        }
        int lastGroup = groups.getInt(0);
        int lastPosition = 0;
        for (int p = 1; p < positionCount; p++) {
            int group = groups.getInt(p);
            if (group != lastGroup) {
                appendRawRange(lastGroup, positionOffset + lastPosition, positionOffset + p, valuesVector, timestampsVector);
                lastGroup = group;
                lastPosition = p;
            }
        }
        appendRawRange(lastGroup, positionOffset + lastPosition, positionOffset + positionCount, valuesVector, timestampsVector);
    }

    private void appendRawRange(int groupId, int from, int to, IntVector valuesVector, LongVector timestampsVector) {
        rawBuffer.prepareForAppend(groupId, to - from, timestampsVector.getLong(from));
        rawBuffer.appendRange(from, to, valuesVector, timestampsVector);
    }

    private void appendRaw(int groupId, long timestamp, int value) {
        rawBuffer.prepareForAppend(groupId, 1, timestamp);
        rawBuffer.appendWithoutResize(timestamp, value);
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
        LongBlock changes = page.getBlock(channels.get(2));
        if (timestamps.areAllValuesNull() || values.areAllValuesNull() || changes.areAllValuesNull()) {
            return;
        }
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            if (timestamps.isNull(valuePosition) || values.isNull(valuePosition) || changes.isNull(valuePosition)) {
                continue;
            }
            addIntermediatePosition(groups.getInt(groupPosition), timestamps, values, changes, valuePosition);
        }
    }

    private void addIntermediateInputBlock(int positionOffset, IntBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        LongBlock timestamps = page.getBlock(channels.get(0));
        IntBlock values = page.getBlock(channels.get(1));
        LongBlock changes = page.getBlock(channels.get(2));
        if (timestamps.areAllValuesNull() || values.areAllValuesNull() || changes.areAllValuesNull()) {
            return;
        }
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            if (groups.isNull(groupPosition)
                || timestamps.isNull(valuePosition)
                || values.isNull(valuePosition)
                || changes.isNull(valuePosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                addIntermediatePosition(groups.getInt(g), timestamps, values, changes, valuePosition);
            }
        }
    }

    private void addIntermediatePosition(int groupId, LongBlock timestamps, IntBlock values, LongBlock changes, int position) {
        ReducedState state = getOrInitializeState(groupId);
        state.appendFromBlocks(timestamps, values, changes, position);
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
            LongBlock.Builder timestamps = blockFactory.newLongBlockBuilder(positionCount);
            IntBlock.Builder values = blockFactory.newIntBlockBuilder(positionCount);
            LongBlock.Builder changes = blockFactory.newLongBlockBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                ReducedState state = group < states.size() ? states.get(group) : null;
                if (state == null) {
                    timestamps.appendNull();
                    values.appendNull();
                    changes.appendNull();
                    continue;
                }
                state.writeToBlocks(timestamps, values, changes);
            }
            blocks[offset] = timestamps.build();
            blocks[offset + 1] = values.build();
            blocks[offset + 2] = changes.build();
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
                ReducedState state = group < states.size() ? states.get(group) : null;
                if (state == null) {
                    changes.appendNull();
                    continue;
                }
                changes.appendLong(state.changes());
            }
            blocks[offset] = changes.build();
        }
    }

    private void flushRawBuffers() {
        if (rawBuffer.minGroupId > rawBuffer.maxGroupId) {
            return;
        }
        states = bigArrays.grow(states, rawBuffer.maxGroupId + 1);
        FlushQueues flushQueues = rawBuffer.prepareForFlush();
        for (int groupId = flushQueues.minGroupId(); groupId <= flushQueues.maxGroupId(); groupId++) {
            FlushQueue flushQueue = flushQueues.getFlushQueue(groupId);
            if (flushQueue == null) {
                continue;
            }
            ReducedState state = getOrInitializeState(groupId);
            flushGroup(state, rawBuffer, flushQueue);
        }
        rawBuffer.clearBuffers();
    }

    private void flushGroup(ReducedState state, IntRawBuffer buffer, FlushQueue flushQueue) {
        LongBuffer timestamps = buffer.timestamps;
        IntBuffer values = buffer.values;
        if (state.canAppendPoints(flushQueue.valueCount)) {
            while (flushQueue.size() > 0) {
                Slice top = flushQueue.top();
                int position = top.next();
                state.appendPoint(timestamps.get(position), values.get(position));
                if (top.exhausted()) {
                    flushQueue.pop();
                } else {
                    flushQueue.updateTop();
                }
            }
            return;
        }

        if (flushQueue.valueCount == 1) {
            long timestamp = timestamps.get(flushQueue.top().start);
            int value = values.get(flushQueue.top().start);
            state.appendInterval(timestamp, value, timestamp, value, 0);
            return;
        }

        final long lastTimestamp;
        final int lastValue;
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

        int previous = lastValue;
        long changes = 0;
        long secondNextTimestamp = flushQueue.secondNextTimestamp();
        while (flushQueue.size() > 1) {
            if (top.lastTimestamp() > secondNextTimestamp) {
                for (int p = top.start; p < top.end; p++) {
                    int value = values.get(p);
                    if (value != previous) {
                        changes++;
                    }
                    previous = value;
                }
                flushQueue.pop();
                top = flushQueue.top();
                secondNextTimestamp = flushQueue.secondNextTimestamp();
                continue;
            }
            int value = values.get(top.next());
            if (value != previous) {
                changes++;
            }
            previous = value;
            if (top.exhausted()) {
                flushQueue.pop();
                top = flushQueue.top();
                secondNextTimestamp = flushQueue.secondNextTimestamp();
            } else if (top.nextTimestamp < secondNextTimestamp) {
                top = flushQueue.updateTop();
                secondNextTimestamp = flushQueue.secondNextTimestamp();
            }
        }

        top = flushQueue.top();
        for (int p = top.start; p < top.end; p++) {
            int value = values.get(p);
            if (value != previous) {
                changes++;
            }
            previous = value;
        }
        state.appendInterval(lastTimestamp, lastValue, timestamps.get(top.end - 1), previous, changes);
    }

    private ReducedState getOrInitializeState(int groupId) {
        states = bigArrays.grow(states, groupId + 1);
        ReducedState state = states.get(groupId);
        if (state == null) {
            state = new ReducedState();
            states.set(groupId, state);
        }
        return state;
    }

    @Override
    public void close() {
        Releasables.close(states, rawBuffer, pointBuffer, intervalBuffer);
    }

    private final class ReducedState {
        private static final int[] EMPTY_INTERVALS = new int[0];
        private static final int[] EMPTY_POINTS = new int[0];
        private static final int MAX_POINT_COUNT = 3;

        private int[] intervals = EMPTY_INTERVALS;
        private int[] points = EMPTY_POINTS;

        long changes() {
            if (intervals.length == 0) {
                sortPoints();
                long changes = 0L;
                for (int i = 1; i < points.length; i++) {
                    if (pointBuffer.value(points[i]) != pointBuffer.value(points[i - 1])) {
                        changes++;
                    }
                }
                return changes;
            }
            compactPointsToInterval();
            sortIntervals();
            if (intervals.length == 0) {
                return 0L;
            }
            long changes = intervalBuffer.changes(intervals[0]);
            for (int i = 1; i < intervals.length; i++) {
                int next = intervals[i - 1];
                int previous = intervals[i];
                changes += intervalBuffer.changes(previous);
                if (intervalBuffer.lastValue(previous) != intervalBuffer.firstValue(next)) {
                    changes++;
                }
            }
            return changes;
        }

        boolean canAppendPoints(int count) {
            return intervals.length == 0 && points.length + count <= MAX_POINT_COUNT;
        }

        void appendPoint(long timestamp, int value) {
            if (canAppendPoints(1) == false) {
                compactPointsToInterval();
                appendInterval(timestamp, value, timestamp, value, 0L);
                return;
            }
            int currentSize = points.length;
            points = ArrayUtil.growExact(points, currentSize + 1);
            points[currentSize] = pointBuffer.append(timestamp, value);
        }

        void appendInterval(long lastTs, int lastValue, long firstTs, int firstValue, long changes) {
            compactPointsToInterval();
            int currentSize = intervals.length;
            intervals = ArrayUtil.growExact(intervals, currentSize + 1);
            intervals[currentSize] = intervalBuffer.appendInterval(lastTs, lastValue, firstTs, firstValue, changes);
        }

        void appendFromBlocks(LongBlock timestamps, IntBlock values, LongBlock changes, int position) {
            int timestampCount = timestamps.getValueCount(position);
            int valueCount = values.getValueCount(position);
            int changeCount = changes.getValueCount(position);
            assert timestampCount == valueCount
                : "timestamps=" + timestamps + "; values=" + values + "; changes=" + changes + "; position=" + position;
            int firstChange = changes.getFirstValueIndex(position);
            if (changeCount == 1 && changes.getLong(firstChange) < 0) {
                assert timestampCount == -changes.getLong(firstChange)
                    : "timestamps=" + timestamps + "; values=" + values + "; changes=" + changes + "; position=" + position;
                int firstTimestamp = timestamps.getFirstValueIndex(position);
                int firstValue = values.getFirstValueIndex(position);
                for (int i = 0; i < timestampCount; i++) {
                    appendPoint(timestamps.getLong(firstTimestamp + i), values.getInt(firstValue + i));
                }
                return;
            }

            assert timestampCount == changeCount * 2
                : "timestamps=" + timestamps + "; values=" + values + "; changes=" + changes + "; position=" + position;
            compactPointsToInterval();
            int firstIntervalId = intervalBuffer.appendIntervalsFromBlocks(timestamps, values, changes, position);
            int currentSize = intervals.length;
            intervals = ArrayUtil.growExact(intervals, currentSize + changeCount);
            for (int i = 0; i < changeCount; i++) {
                intervals[currentSize++] = firstIntervalId + i;
            }
        }

        void writeToBlocks(LongBlock.Builder timestamps, IntBlock.Builder values, LongBlock.Builder changes) {
            if (intervals.length == 0) {
                sortPoints();
                timestamps.beginPositionEntry();
                values.beginPositionEntry();
                for (int point : points) {
                    timestamps.appendLong(pointBuffer.timestamp(point));
                    values.appendInt(pointBuffer.value(point));
                }
                timestamps.endPositionEntry();
                values.endPositionEntry();
                changes.appendLong(-points.length);
                return;
            }

            compactPointsToInterval();
            sortIntervals();
            timestamps.beginPositionEntry();
            values.beginPositionEntry();
            changes.beginPositionEntry();
            for (int intervalId : intervals) {
                timestamps.appendLong(intervalBuffer.lastTs(intervalId));
                timestamps.appendLong(intervalBuffer.firstTs(intervalId));
                values.appendInt(intervalBuffer.lastValue(intervalId));
                values.appendInt(intervalBuffer.firstValue(intervalId));
                changes.appendLong(intervalBuffer.changes(intervalId));
            }
            timestamps.endPositionEntry();
            values.endPositionEntry();
            changes.endPositionEntry();
        }

        private void compactPointsToInterval() {
            if (points.length == 0) {
                return;
            }
            sortPoints();
            long changes = 0L;
            for (int i = 1; i < points.length; i++) {
                if (pointBuffer.value(points[i]) != pointBuffer.value(points[i - 1])) {
                    changes++;
                }
            }
            int last = points[0];
            int first = points[points.length - 1];
            int currentSize = intervals.length;
            intervals = ArrayUtil.growExact(intervals, currentSize + 1);
            intervals[currentSize] = intervalBuffer.appendInterval(
                pointBuffer.timestamp(last),
                pointBuffer.value(last),
                pointBuffer.timestamp(first),
                pointBuffer.value(first),
                changes
            );
            points = EMPTY_POINTS;
        }

        private void sortPoints() {
            new IntroSorter() {
                private long pivotTimestamp;

                @Override
                protected void setPivot(int i) {
                    pivotTimestamp = pointBuffer.timestamp(points[i]);
                }

                @Override
                protected int comparePivot(int j) {
                    return Long.compare(pointBuffer.timestamp(points[j]), pivotTimestamp);
                }

                @Override
                protected int compare(int i, int j) {
                    return Long.compare(pointBuffer.timestamp(points[j]), pointBuffer.timestamp(points[i]));
                }

                @Override
                protected void swap(int i, int j) {
                    int tmp = points[i];
                    points[i] = points[j];
                    points[j] = tmp;
                }
            }.sort(0, points.length);
        }

        private void sortIntervals() {
            new IntroSorter() {
                private long pivotTimestamp;

                @Override
                protected void setPivot(int i) {
                    pivotTimestamp = intervalBuffer.lastTs(intervals[i]);
                }

                @Override
                protected int comparePivot(int j) {
                    return Long.compare(intervalBuffer.lastTs(intervals[j]), pivotTimestamp);
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

    private static final class IntRawBuffer extends RawBuffer {
        private final IntBuffer values;

        IntRawBuffer(org.elasticsearch.common.breaker.CircuitBreaker breaker) {
            super(breaker);
            boolean success = false;
            try {
                this.values = new IntBuffer(breaker, PAGE_SIZE);
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

        void appendWithoutResize(long timestamp, int value) {
            timestamps.append(timestamp);
            values.append(value);
        }

        void appendRange(int fromPosition, int toPosition, IntVector valuesVector, LongVector timestampsVector) {
            int count = toPosition - fromPosition;
            timestamps.appendRange(timestampsVector, fromPosition, count);
            values.appendRange(valuesVector, fromPosition, count);
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

    private static final class PointBuffer implements org.elasticsearch.core.Releasable {
        private final LongBuffer timestamps;
        private final IntBuffer values;

        PointBuffer(org.elasticsearch.common.breaker.CircuitBreaker breaker) {
            LongBuffer timestamps = null;
            IntBuffer values = null;
            boolean success = false;
            try {
                timestamps = new LongBuffer(breaker, PAGE_SIZE);
                values = new IntBuffer(breaker, PAGE_SIZE);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(timestamps, values);
                }
            }
            this.timestamps = timestamps;
            this.values = values;
        }

        int append(long timestamp, int value) {
            int id = timestamps.size();
            timestamps.ensureCapacity(id + 1);
            values.ensureCapacity(id + 1);
            timestamps.append(timestamp);
            values.append(value);
            return id;
        }

        long timestamp(int point) {
            return timestamps.get(point);
        }

        int value(int point) {
            return values.get(point);
        }

        @Override
        public void close() {
            Releasables.close(timestamps, values);
        }
    }

    private static final class IntervalBuffer implements org.elasticsearch.core.Releasable {
        private final LongBuffer timestamps;
        private final IntBuffer values;
        private final LongBuffer changes;

        IntervalBuffer(org.elasticsearch.common.breaker.CircuitBreaker breaker) {
            LongBuffer timestamps = null;
            IntBuffer values = null;
            LongBuffer changes = null;
            boolean success = false;
            try {
                timestamps = new LongBuffer(breaker, PAGE_SIZE);
                values = new IntBuffer(breaker, PAGE_SIZE);
                changes = new LongBuffer(breaker, PAGE_SIZE);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(timestamps, values, changes);
                }
            }
            this.timestamps = timestamps;
            this.values = values;
            this.changes = changes;
        }

        int count() {
            return changes.size();
        }

        long lastTs(int intervalId) {
            return timestamps.get(2 * intervalId);
        }

        int lastValue(int intervalId) {
            return values.get(2 * intervalId);
        }

        long firstTs(int intervalId) {
            return timestamps.get(2 * intervalId + 1);
        }

        int firstValue(int intervalId) {
            return values.get(2 * intervalId + 1);
        }

        long changes(int intervalId) {
            return changes.get(intervalId);
        }

        int appendInterval(long lastTs, int lastValue, long firstTs, int firstValue, long changeCount) {
            int id = count();
            timestamps.ensureCapacity(timestamps.size() + 2);
            values.ensureCapacity(values.size() + 2);
            changes.ensureCapacity(changes.size() + 1);
            timestamps.append(lastTs);
            values.append(lastValue);
            timestamps.append(firstTs);
            values.append(firstValue);
            changes.append(changeCount);
            return id;
        }

        int appendIntervalsFromBlocks(LongBlock ts, IntBlock vs, LongBlock cs, int position) {
            int timestampFirst = ts.getFirstValueIndex(position);
            int valueFirst = vs.getFirstValueIndex(position);
            int changesFirst = cs.getFirstValueIndex(position);
            int intervalCount = cs.getValueCount(position);

            timestamps.ensureCapacity(timestamps.size() + intervalCount * 2);
            values.ensureCapacity(values.size() + intervalCount * 2);
            changes.ensureCapacity(changes.size() + intervalCount);

            int firstId = count();
            for (int i = 0; i < intervalCount; i++) {
                timestamps.append(ts.getLong(timestampFirst + 2 * i));
                values.append(vs.getInt(valueFirst + 2 * i));
                timestamps.append(ts.getLong(timestampFirst + 2 * i + 1));
                values.append(vs.getInt(valueFirst + 2 * i + 1));
                changes.append(cs.getLong(changesFirst + i));
            }
            return firstId;
        }

        @Override
        public void close() {
            Releasables.close(timestamps, values, changes);
        }
    }
}
