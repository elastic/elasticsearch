/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.util.List;

/** {@link AggregatorFunctionSupplier} implementation for PromQL changes over exponential histogram values. */
public final class ChangesExponentialHistogramAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return ChangesExponentialHistogramGroupingAggregatorFunction.intermediateStateDesc();
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        return new ChangesExponentialHistogramGroupingAggregatorFunction(channels, driverContext);
    }

    @Override
    public String describe() {
        return "changes of exponential histograms";
    }

    static final class ChangesExponentialHistogramGroupingAggregatorFunction extends AbstractRateGroupingFunction
        implements
            GroupingAggregatorFunction {
        private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
            new IntermediateStateDesc("timestamps", ElementType.LONG),
            new IntermediateStateDesc("values", ElementType.EXPONENTIAL_HISTOGRAM)
        );

        private final List<Integer> channels;
        private final DriverContext driverContext;
        private final ExponentialHistogramRawBuffer rawBuffer;
        private FlushQueues preparedFlushQueues;

        ChangesExponentialHistogramGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
            this.channels = channels;
            this.driverContext = driverContext;
            ExponentialHistogramRawBuffer rawBuffer = null;
            try {
                rawBuffer = new ExponentialHistogramRawBuffer(driverContext.blockFactory());
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
            ExponentialHistogramBlock values = page.getBlock(channels.get(0));
            LongBlock timestamps = page.getBlock(channels.get(1));
            if (values.areAllValuesNull() || timestamps.areAllValuesNull()) {
                return null;
            }
            return new AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groups) {
                    addRawInput(positionOffset, groups, values, timestamps);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groups) {
                    addRawInput(positionOffset, groups, values, timestamps);
                }

                @Override
                public void add(int positionOffset, IntVector groups) {
                    addRawInput(positionOffset, groups, values, timestamps);
                }

                @Override
                public void close() {}
            };
        }

        private void addRawInput(int positionOffset, IntBlock groups, ExponentialHistogramBlock values, LongBlock timestamps) {
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
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
                    appendValues(groups.getInt(g), timestamps, timestampStart, timestampEnd, values, valueStart, valueEnd, scratch);
                }
            }
        }

        private void addRawInput(int positionOffset, IntVector groups, ExponentialHistogramBlock values, LongBlock timestamps) {
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (values.isNull(valuePosition) || timestamps.isNull(valuePosition)) {
                    continue;
                }
                int valueStart = values.getFirstValueIndex(valuePosition);
                int valueEnd = valueStart + values.getValueCount(valuePosition);
                int timestampStart = timestamps.getFirstValueIndex(valuePosition);
                int timestampEnd = timestampStart + timestamps.getValueCount(valuePosition);
                appendValues(groups.getInt(p), timestamps, timestampStart, timestampEnd, values, valueStart, valueEnd, scratch);
            }
        }

        private void appendValues(
            int groupId,
            LongBlock timestamps,
            int timestampStart,
            int timestampEnd,
            ExponentialHistogramBlock values,
            int valueStart,
            int valueEnd,
            ExponentialHistogramScratch scratch
        ) {
            for (int t = timestampStart; t < timestampEnd; t++) {
                long timestamp = timestamps.getLong(t);
                for (int v = valueStart; v < valueEnd; v++) {
                    rawBuffer.append(groupId, timestamp, values.getExponentialHistogram(v, scratch));
                }
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
            ExponentialHistogramBlock values = page.getBlock(channels.get(1));
            if (timestamps.areAllValuesNull() || values.areAllValuesNull()) {
                return;
            }
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (timestamps.isNull(valuePosition) == false && values.isNull(valuePosition) == false) {
                    addIntermediatePosition(groups.getInt(p), timestamps, values, valuePosition, scratch);
                }
            }
        }

        private void addIntermediateInputBlock(int positionOffset, IntBlock groups, Page page) {
            assert channels.size() == intermediateBlockCount();
            LongBlock timestamps = page.getBlock(channels.get(0));
            ExponentialHistogramBlock values = page.getBlock(channels.get(1));
            if (timestamps.areAllValuesNull() || values.areAllValuesNull()) {
                return;
            }
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (groups.isNull(p) || timestamps.isNull(valuePosition) || values.isNull(valuePosition)) {
                    continue;
                }
                int groupStart = groups.getFirstValueIndex(p);
                int groupEnd = groupStart + groups.getValueCount(p);
                for (int g = groupStart; g < groupEnd; g++) {
                    addIntermediatePosition(groups.getInt(g), timestamps, values, valuePosition, scratch);
                }
            }
        }

        private void addIntermediatePosition(
            int groupId,
            LongBlock timestamps,
            ExponentialHistogramBlock values,
            int position,
            ExponentialHistogramScratch scratch
        ) {
            int count = timestamps.getValueCount(position);
            assert count == values.getValueCount(position) : "timestamps=" + timestamps + "; values=" + values + "; position=" + position;
            int firstTimestamp = timestamps.getFirstValueIndex(position);
            int firstValue = values.getFirstValueIndex(position);
            for (int i = 0; i < count; i++) {
                rawBuffer.append(groupId, timestamps.getLong(firstTimestamp + i), values.getExponentialHistogram(firstValue + i, scratch));
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
                ExponentialHistogramBlock.Builder values = driverContext.blockFactory().newExponentialHistogramBlockBuilder(positionCount)
            ) {
                ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
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
                            values.append(rawBuffer.value(position, scratch));
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
            ExponentialHistogramScratch leftScratch = new ExponentialHistogramScratch();
            ExponentialHistogramScratch rightScratch = new ExponentialHistogramScratch();
            int previous = nextPosition(flushQueue);
            long changes = 0;
            while (flushQueue.size() > 0) {
                int current = nextPosition(flushQueue);
                ExponentialHistogram left = rawBuffer.value(previous, leftScratch);
                ExponentialHistogram right = rawBuffer.value(current, rightScratch);
                if (histogramsEqual(left, right) == false) {
                    changes++;
                }
                previous = current;
            }
            return changes;
        }

        private static int compareHistograms(ExponentialHistogram left, ExponentialHistogram right) {
            if (histogramsEqual(left, right)) {
                return 0;
            }
            int result = Integer.compare(left.scale(), right.scale());
            if (result == 0) {
                result = Long.compare(left.valueCount(), right.valueCount());
            }
            if (result == 0) {
                result = compareRawDouble(left.sum(), right.sum());
            }
            if (result == 0) {
                result = compareZeroBuckets(left.zeroBucket(), right.zeroBucket());
            }
            if (result == 0) {
                result = compareBuckets(left.negativeBuckets().iterator(), right.negativeBuckets().iterator());
            }
            if (result == 0) {
                result = compareBuckets(left.positiveBuckets().iterator(), right.positiveBuckets().iterator());
            }
            return result;
        }

        private static boolean histogramsEqual(ExponentialHistogram left, ExponentialHistogram right) {
            return left.scale() == right.scale()
                && left.valueCount() == right.valueCount()
                && Double.doubleToRawLongBits(left.sum()) == Double.doubleToRawLongBits(right.sum())
                && left.zeroBucket().zeroThreshold() == right.zeroBucket().zeroThreshold()
                && left.zeroBucket().count() == right.zeroBucket().count()
                && compareBuckets(left.negativeBuckets().iterator(), right.negativeBuckets().iterator()) == 0
                && compareBuckets(left.positiveBuckets().iterator(), right.positiveBuckets().iterator()) == 0;
        }

        private static int compareRawDouble(double left, double right) {
            int result = Double.compare(left, right);
            if (result == 0) {
                result = Long.compareUnsigned(Double.doubleToRawLongBits(left), Double.doubleToRawLongBits(right));
            }
            return result;
        }

        private static int compareZeroBuckets(ZeroBucket left, ZeroBucket right) {
            int result = left.zeroThreshold() == right.zeroThreshold() ? 0 : Double.compare(left.zeroThreshold(), right.zeroThreshold());
            if (result == 0) {
                result = Long.compare(left.count(), right.count());
            }
            return result;
        }

        private static int compareBuckets(BucketIterator left, BucketIterator right) {
            int result = Integer.compare(left.scale(), right.scale());
            while (result == 0 && left.hasNext() && right.hasNext()) {
                result = Long.compare(left.peekIndex(), right.peekIndex());
                if (result == 0) {
                    result = Long.compare(left.peekCount(), right.peekCount());
                }
                left.advance();
                right.advance();
            }
            if (result == 0) {
                result = Boolean.compare(left.hasNext(), right.hasNext());
            }
            return result;
        }

        @Override
        public void close() {
            Releasables.close(rawBuffer);
        }

        private static final class ExponentialHistogramRawBuffer extends RawBuffer {
            private final ExponentialHistogramBuffer values;
            private final ExponentialHistogramScratch leftScratch = new ExponentialHistogramScratch();
            private final ExponentialHistogramScratch rightScratch = new ExponentialHistogramScratch();

            ExponentialHistogramRawBuffer(org.elasticsearch.compute.data.BlockFactory blockFactory) {
                super(blockFactory.breaker());
                boolean success = false;
                try {
                    this.values = new ExponentialHistogramBuffer(blockFactory, PAGE_SIZE);
                    success = true;
                } finally {
                    if (success == false) {
                        close();
                    }
                }
            }

            void append(int groupId, long timestamp, ExponentialHistogram value) {
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

            ExponentialHistogram value(int position, ExponentialHistogramScratch scratch) {
                return values.get(position, scratch);
            }

            int compareValues(int leftPosition, int rightPosition) {
                ExponentialHistogram left = value(leftPosition, leftScratch);
                ExponentialHistogram right = value(rightPosition, rightScratch);
                return compareHistograms(left, right);
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
}
