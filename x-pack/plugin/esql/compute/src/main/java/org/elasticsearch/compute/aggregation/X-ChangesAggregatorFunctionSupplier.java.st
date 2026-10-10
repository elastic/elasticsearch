/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.List;
// end generated imports

/**
 * {@link AggregatorFunctionSupplier} implementation for PromQL changes over $type$ values.
 * This class is generated. Edit {@code X-ChangesAggregatorFunctionSupplier.java.st} instead.
 */
public final class Changes$Type$AggregatorFunctionSupplier implements AggregatorFunctionSupplier {
    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return Changes$Type$GroupingAggregatorFunction.intermediateStateDesc();
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        return new Changes$Type$GroupingAggregatorFunction(channels, driverContext);
    }

    @Override
    public String describe() {
        return "changes of $type$s";
    }
}

final class Changes$Type$GroupingAggregatorFunction extends AbstractRateGroupingFunction implements GroupingAggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("timestamps", ElementType.LONG),
        new IntermediateStateDesc("values", ElementType.$TYPE$)
    );

    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final $Type$RawBuffer rawBuffer;
    private FlushQueues preparedFlushQueues;

    Changes$Type$GroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels;
        this.driverContext = driverContext;
        $Type$RawBuffer rawBuffer = null;
        try {
            rawBuffer = new $Type$RawBuffer(driverContext.breaker());
            this.rawBuffer = rawBuffer;
            rawBuffer = null;
        } finally {
            Releasables.close(rawBuffer);
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
        // Nulls are represented by missing flush queues.
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        $Type$Block valuesBlock = page.getBlock(channels.get(0));
        LongBlock timestampsBlock = page.getBlock(channels.get(1));
        if (valuesBlock.areAllValuesNull() || timestampsBlock.areAllValuesNull()) {
            return null;
        }
        $Type$Vector valuesVector = valuesBlock.asVector();
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
                public void close() {}
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
            public void close() {}
        };
    }

    private void addRawInput(int positionOffset, IntBlock groups, $Type$Block values, LongBlock timestamps) {
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
                int groupId = groups.getInt(g);
                for (int t = timestampStart; t < timestampEnd; t++) {
                    long timestamp = timestamps.getLong(t);
                    for (int v = valueStart; v < valueEnd; v++) {
                        rawBuffer.append(groupId, timestamp, values.get$Type$(v));
                    }
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, $Type$Block values, LongBlock timestamps) {
        for (int p = 0; p < groups.getPositionCount(); p++) {
            int valuePosition = positionOffset + p;
            if (values.isNull(valuePosition) || timestamps.isNull(valuePosition)) {
                continue;
            }
            int groupId = groups.getInt(p);
            int valueStart = values.getFirstValueIndex(valuePosition);
            int valueEnd = valueStart + values.getValueCount(valuePosition);
            int timestampStart = timestamps.getFirstValueIndex(valuePosition);
            int timestampEnd = timestampStart + timestamps.getValueCount(valuePosition);
            for (int t = timestampStart; t < timestampEnd; t++) {
                long timestamp = timestamps.getLong(t);
                for (int v = valueStart; v < valueEnd; v++) {
                    rawBuffer.append(groupId, timestamp, values.get$Type$(v));
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntBlock groups, $Type$Vector values, LongVector timestamps) {
        for (int p = 0; p < groups.getPositionCount(); p++) {
            if (groups.isNull(p)) {
                continue;
            }
            int valuePosition = positionOffset + p;
            $type$ value = values.get$Type$(valuePosition);
            long timestamp = timestamps.getLong(valuePosition);
            int groupStart = groups.getFirstValueIndex(p);
            int groupEnd = groupStart + groups.getValueCount(p);
            for (int g = groupStart; g < groupEnd; g++) {
                rawBuffer.append(groups.getInt(g), timestamp, value);
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, $Type$Vector values, LongVector timestamps) {
        for (int p = 0; p < groups.getPositionCount(); p++) {
            int valuePosition = positionOffset + p;
            rawBuffer.append(groups.getInt(p), timestamps.getLong(valuePosition), values.get$Type$(valuePosition));
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
        $Type$Block values = page.getBlock(channels.get(1));
        if (timestamps.areAllValuesNull() || values.areAllValuesNull()) {
            return;
        }
        for (int p = 0; p < groups.getPositionCount(); p++) {
            int valuePosition = positionOffset + p;
            if (timestamps.isNull(valuePosition) == false && values.isNull(valuePosition) == false) {
                addIntermediatePosition(groups.getInt(p), timestamps, values, valuePosition);
            }
        }
    }

    private void addIntermediateInputBlock(int positionOffset, IntBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        LongBlock timestamps = page.getBlock(channels.get(0));
        $Type$Block values = page.getBlock(channels.get(1));
        if (timestamps.areAllValuesNull() || values.areAllValuesNull()) {
            return;
        }
        for (int p = 0; p < groups.getPositionCount(); p++) {
            int valuePosition = positionOffset + p;
            if (groups.isNull(p) || timestamps.isNull(valuePosition) || values.isNull(valuePosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(p);
            int groupEnd = groupStart + groups.getValueCount(p);
            for (int g = groupStart; g < groupEnd; g++) {
                addIntermediatePosition(groups.getInt(g), timestamps, values, valuePosition);
            }
        }
    }

    private void addIntermediatePosition(int groupId, LongBlock timestamps, $Type$Block values, int position) {
        int count = timestamps.getValueCount(position);
        assert count == values.getValueCount(position) : "timestamps=" + timestamps + "; values=" + values + "; position=" + position;
        int firstTimestamp = timestamps.getFirstValueIndex(position);
        int firstValue = values.getFirstValueIndex(position);
        for (int i = 0; i < count; i++) {
            rawBuffer.append(groupId, timestamps.getLong(firstTimestamp + i), values.get$Type$(firstValue + i));
        }
    }

    @Override
    public PreparedForEvaluation prepareEvaluateIntermediate(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        flushQueues();
        return this::evaluateIntermediate;
    }

    private void evaluateIntermediate(Block[] blocks, int offset, IntVector selectedInPage) {
        int positionCount = selectedInPage.getPositionCount();
        try (
            LongBlock.Builder timestamps = driverContext.blockFactory().newLongBlockBuilder(positionCount);
            $Type$Block.Builder values = driverContext.blockFactory().new$Type$BlockBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                FlushQueue flushQueue = preparedFlushQueues.getFlushQueue(group, rawBuffer::compareValues);
                if (flushQueue == null) {
                    timestamps.appendNull();
                    values.appendNull();
                } else {
                    timestamps.beginPositionEntry();
                    values.beginPositionEntry();
                    while (flushQueue.size() > 0) {
                        int position = nextPosition(flushQueue);
                        timestamps.appendLong(rawBuffer.timestamp(position));
                        values.append$Type$(rawBuffer.value(position));
                    }
                    timestamps.endPositionEntry();
                    values.endPositionEntry();
                }
            }
            blocks[offset] = timestamps.build();
            blocks[offset + 1] = values.build();
        }
    }

    @Override
    public PreparedForEvaluation prepareEvaluateFinal(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        flushQueues();
        return this::evaluateFinal;
    }

    private void evaluateFinal(Block[] blocks, int offset, IntVector selectedInPage) {
        int positionCount = selectedInPage.getPositionCount();
        try (LongBlock.Builder changes = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                FlushQueue flushQueue = preparedFlushQueues.getFlushQueue(group, rawBuffer::compareValues);
                if (flushQueue == null) {
                    changes.appendNull();
                } else {
                    changes.appendLong(countChanges(flushQueue));
                }
            }
            blocks[offset] = changes.build();
        }
    }

    private FlushQueues flushQueues() {
        if (preparedFlushQueues == null) {
            preparedFlushQueues = rawBuffer.prepareForFlush();
        }
        return preparedFlushQueues;
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

    private long countChanges(FlushQueue flushQueue) {
        int previous = nextPosition(flushQueue);
        long changes = 0;
        while (flushQueue.size() > 0) {
            int current = nextPosition(flushQueue);
            if (valuesEqual(rawBuffer.value(previous), rawBuffer.value(current)) == false) {
                changes++;
            }
            previous = current;
        }
        return changes;
    }

    private static boolean valuesEqual($type$ left, $type$ right) {
        return left == right$if(double)$ || (Double.isNaN(left) && Double.isNaN(right))$endif$;
    }

    @Override
    public void close() {
        Releasables.close(rawBuffer);
    }

    private static final class $Type$RawBuffer extends RawBuffer {
        private final $Type$Buffer values;

        $Type$RawBuffer(org.elasticsearch.common.breaker.CircuitBreaker breaker) {
            super(breaker);
            boolean success = false;
            try {
                this.values = new $Type$Buffer(breaker, PAGE_SIZE);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        void append(int groupId, long timestamp, $type$ value) {
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

        $type$ value(int position) {
            return values.get(position);
        }

        int compareValues(int leftPosition, int rightPosition) {
            return $if(double)$Double.compare(value(leftPosition), value(rightPosition))$else$Long.compare(value(leftPosition), value(rightPosition))$endif$;
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
