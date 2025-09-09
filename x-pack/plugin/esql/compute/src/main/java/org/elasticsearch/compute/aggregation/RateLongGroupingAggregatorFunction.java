/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.aggregation.oldrate.OldRateDoubleGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.oldrate.OldRateLongAggregator;
import org.elasticsearch.compute.aggregation.oldrate.OldRateLongGroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
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

public final class RateLongGroupingAggregatorFunction implements GroupingAggregatorFunction {

    public static final class FunctionSupplier implements AggregatorFunctionSupplier {
        public FunctionSupplier() {

        }

        @Override
        public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
            throw new UnsupportedOperationException("non-grouping aggregator is not supported");
        }

        @Override
        public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
            return RateDoubleGroupingAggregatorFunction.intermediateStateDesc();
        }

        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
            throw new UnsupportedOperationException("non-grouping aggregator is not supported");
        }

        @Override
        public RateLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new RateLongGroupingAggregatorFunction(channels, driverContext);
        }

        @Override
        public String describe() {
            return "rate of longs";
        }
    }

    private final List<Integer> channels;

    private final DriverContext driverContext;
    private ObjectArray<Buffer> buffers;
    private final OldRateLongAggregator.LongRateGroupingState oldState;
    private final OldRateLongGroupingAggregatorFunction oldRate;

    public RateLongGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.buffers = driverContext.bigArrays().newObjectArray(256);
        this.oldState = new OldRateLongAggregator.LongRateGroupingState(driverContext.bigArrays(), driverContext.breaker());
        this.oldRate = new OldRateLongGroupingAggregatorFunction(channels, oldState, driverContext);
    }

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return OldRateDoubleGroupingAggregatorFunction.intermediateStateDesc();
    }

    @Override
    public int intermediateBlockCount() {
        return intermediateStateDesc().size();
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        LongBlock valuesBlock = page.getBlock(channels.get(0));
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
                int groupId = groups.getInt(g);
                if (lastGroup != groupId) {
                    buffer = getBuffer(groupId, positionCount - p, timestamp);
                    lastGroup = groupId;
                }
                int valueStart = valueBlock.getFirstValueIndex(valuePosition);
                buffer.appendOneValue(timestamp, valueBlock.getLong(valueStart));
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
                buffer.appendOneValue(timestampVector.getLong(valuePosition), valueBlock.getLong(valuePosition));
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
                int groupId = groups.getInt(p);
                if (lastGroup != groupId) {
                    buffer = getBuffer(groupId, groups.getPositionCount() - p, timestamp);
                    lastGroup = groupId;
                }
                buffer.appendOneValue(timestamp, valueBlock.getLong(valuePosition));
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, LongVector valueVector, LongVector timestampVector) {
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            Buffer state = getBuffer(groupId, positionCount, timestampVector.getLong(0));
            for (int p = 0; p < positionCount; p++) {
                int valuePosition = positionOffset + p;
                state.appendOneValue(timestampVector.getLong(valuePosition), valueVector.getLong(valuePosition));
            }
        } else {
            int lastGroup = -1;
            Buffer buffer = null;
            for (int p = 0; p < positionCount; p++) {
                int valuePosition = positionOffset + p;
                long timestamp = timestampVector.getLong(valuePosition);
                int groupId = groups.getInt(p);
                if (lastGroup != groupId) {
                    buffer = getBuffer(groupId, positionCount - p, timestamp);
                    lastGroup = groupId;
                }
                buffer.appendOneValue(timestamp, valueVector.getLong(valuePosition));
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        oldRate.addIntermediateInput(positionOffset, groups, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        oldRate.addIntermediateInput(positionOffset, groups, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        oldRate.addIntermediateInput(positionOffset, groups, page);
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        oldRate.selectedMayContainUnseenGroups(seenGroupIds);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        flushBuffers(selected);
        oldRate.evaluateIntermediate(blocks, offset, selected);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        flushBuffers(selected);
        oldRate.evaluateFinal(blocks, offset, selected, ctx);
    }

    void flushBuffers(IntVector selected) {
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int groupId = selected.getInt(i);
            if (groupId < buffers.size()) {
                var buffer = buffers.getAndSet(groupId, null);
                if (buffer != null) {
                    try (buffer) {
                        flushBufferToOldRate(buffer, groupId);
                    }
                }
            }
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

    @Override
    public void close() {
        for (long i = 0; i < buffers.size(); i++) {
            Buffer buffer = buffers.get(i);
            if (buffer != null) {
                buffer.close();
            }
        }
        buffers.close();
        Releasables.close(oldRate);
    }

    private static class Slice {
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

    /**
     * Flushes the buffering data points to the old rate state.
     */
    void flushBufferToOldRate(Buffer buffer, int groupId) {
        if (buffer.totalCount == 1) {
            try (
                var ts = driverContext.blockFactory().newConstantLongVector(buffer.timestamps.get(0), 1);
                var vs = driverContext.blockFactory().newConstantLongVector(buffer.values.get(0), 1)
            ) {
                oldState.combine(groupId, ts.asBlock(), vs.asBlock(), 1, 0.0, 0);
            }
            return;
        }
        var pq = buffer.mergeQueue();
        // first
        final long lastTimestamp;
        final long lastValue;
        {
            Slice top = pq.top();
            int position = top.next();
            if (top.exhausted()) {
                pq.pop();
            } else {
                pq.updateTop();
            }
            lastTimestamp = buffer.timestamps.get(position);
            lastValue = buffer.values.get(position);
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
            var val = buffer.values.get(position);
            reset += dv(val, prevValue) + dv(prevValue, lastValue) - dv(val, lastValue);
            prevValue = val;
        }
        try (
            var tBuilder = driverContext.blockFactory().newLongBlockBuilder(2);
            var vBuilder = driverContext.blockFactory().newLongBlockBuilder(2)
        ) {
            tBuilder.beginPositionEntry();
            tBuilder.appendLong(lastTimestamp);
            tBuilder.appendLong(buffer.timestamps.get(position));
            tBuilder.endPositionEntry();

            vBuilder.beginPositionEntry();
            vBuilder.appendLong(lastValue);
            vBuilder.appendLong(buffer.values.get(position));
            vBuilder.endPositionEntry();

            try (var ts = tBuilder.build(); var vs = vBuilder.build()) {
                oldState.combine(groupId, ts, vs, buffer.totalCount, reset, 0);
            }
        }
    }

    // TODO: copied from old rate - simplify this or explain why we need it?
    private double dv(double v0, double v1) {
        return v0 > v1 ? v1 : v1 - v0;
    }

    private Buffer getBuffer(int groupId, int extraSize, long firstTimestamp) {
        buffers = driverContext.bigArrays().grow(buffers, groupId + 1);
        Buffer state = buffers.get(groupId);
        if (state == null) {
            state = new Buffer(driverContext.bigArrays(), Math.max(16, extraSize));
            buffers.set(groupId, state);
        } else {
            state.ensureCapacity(driverContext.bigArrays(), extraSize, firstTimestamp);
        }
        return state;
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
        private int totalCount;
        int[] sliceOffsets;
        private static final int[] EMPTY_SLICES = new int[0];

        Buffer(BigArrays bigArrays, int initialSize) {
            this.timestamps = bigArrays.newLongArray(initialSize, false);
            this.values = bigArrays.newLongArray(initialSize, false);
            this.sliceOffsets = EMPTY_SLICES;
        }

        void appendOneValue(long timestamp, long value) {
            timestamps.set(totalCount, timestamp);
            values.set(totalCount, value);
            totalCount++;
        }

        void ensureCapacity(BigArrays bigArrays, int count, long firstTimestamp) {
            int newSize = totalCount + count;
            timestamps = bigArrays.grow(timestamps, newSize);
            values = bigArrays.grow(values, newSize);
            if (totalCount > 0 && firstTimestamp > timestamps.get(totalCount - 1)) {
                sliceOffsets = ArrayUtil.growExact(sliceOffsets, sliceOffsets.length + 1);
                sliceOffsets[sliceOffsets.length - 1] = totalCount;
            }
        }

        PriorityQueue<Slice> mergeQueue() {
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
            pq.add(new Slice(this, startOffset, totalCount));
            return pq;
        }

        @Override
        public void close() {
            timestamps.close();
            values.close();
        }
    }

}
