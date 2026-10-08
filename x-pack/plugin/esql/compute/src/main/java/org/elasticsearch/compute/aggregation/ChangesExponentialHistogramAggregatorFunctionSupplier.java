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
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
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
        private final BigArrays bigArrays;
        private final PointBuffer points;
        private ObjectArray<ReducedState> states;

        ChangesExponentialHistogramGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
            this.channels = channels;
            this.driverContext = driverContext;
            this.bigArrays = driverContext.bigArrays();
            PointBuffer points = null;
            try {
                points = new PointBuffer(driverContext.blockFactory(), driverContext.breaker());
                this.states = bigArrays.newObjectArray(256);
                this.points = points;
                points = null;
            } finally {
                Releasables.close(points);
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
                    ReducedState state = getOrInitializeState(groups.getInt(g));
                    appendValues(state, timestamps, timestampStart, timestampEnd, values, valueStart, valueEnd, scratch);
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
                ReducedState state = getOrInitializeState(groups.getInt(p));
                int valueStart = values.getFirstValueIndex(valuePosition);
                int valueEnd = valueStart + values.getValueCount(valuePosition);
                int timestampStart = timestamps.getFirstValueIndex(valuePosition);
                int timestampEnd = timestampStart + timestamps.getValueCount(valuePosition);
                appendValues(state, timestamps, timestampStart, timestampEnd, values, valueStart, valueEnd, scratch);
            }
        }

        private static void appendValues(
            ReducedState state,
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
                    state.append(timestamp, values.getExponentialHistogram(v, scratch));
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
            ReducedState state = getOrInitializeState(groupId);
            for (int i = 0; i < count; i++) {
                state.append(timestamps.getLong(firstTimestamp + i), values.getExponentialHistogram(firstValue + i, scratch));
            }
        }

        @Override
        public PreparedForEvaluation prepareEvaluateIntermediate(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
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
                    ReducedState state = group < states.size() ? states.get(group) : null;
                    if (state == null) {
                        timestamps.appendNull();
                        values.appendNull();
                    } else {
                        timestamps.beginPositionEntry();
                        values.beginPositionEntry();
                        for (int point = state.head; point >= 0; point = points.next(point)) {
                            timestamps.appendLong(points.timestamp(point));
                            values.append(points.value(point, scratch));
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
                    } else {
                        changes.appendLong(state.changes());
                    }
                }
                blocks[offset] = changes.build();
            }
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
            Releasables.close(states, points);
        }

        private final class ReducedState {
            private final ExponentialHistogramScratch leftScratch = new ExponentialHistogramScratch();
            private final ExponentialHistogramScratch rightScratch = new ExponentialHistogramScratch();
            private int head = -1;
            private int count;

            void append(long timestamp, ExponentialHistogram value) {
                head = points.append(timestamp, value, head);
                count++;
            }

            long changes() {
                if (count < 2) {
                    return 0;
                }
                long bytes = (long) count * Integer.BYTES;
                driverContext.breaker().addEstimateBytesAndMaybeBreak(bytes, "changes-sort");
                try {
                    int[] sorted = new int[count];
                    int point = head;
                    for (int i = 0; i < count; i++) {
                        sorted[i] = point;
                        point = points.next(point);
                    }
                    new IntroSorter() {
                        private int pivotPoint;

                        @Override
                        protected void setPivot(int i) {
                            pivotPoint = sorted[i];
                        }

                        @Override
                        protected int comparePivot(int j) {
                            return comparePoints(pivotPoint, sorted[j]);
                        }

                        @Override
                        protected int compare(int i, int j) {
                            return comparePoints(sorted[i], sorted[j]);
                        }

                        @Override
                        protected void swap(int i, int j) {
                            int tmp = sorted[i];
                            sorted[i] = sorted[j];
                            sorted[j] = tmp;
                        }
                    }.sort(0, count);
                    long changes = 0;
                    for (int i = 1; i < count; i++) {
                        ExponentialHistogram left = points.value(sorted[i], leftScratch);
                        ExponentialHistogram right = points.value(sorted[i - 1], rightScratch);
                        if (histogramsEqual(left, right) == false) {
                            changes++;
                        }
                    }
                    return changes;
                } finally {
                    driverContext.breaker().addWithoutBreaking(-bytes);
                }
            }

            private int comparePoints(int leftPoint, int rightPoint) {
                int timestampOrder = Long.compare(points.timestamp(rightPoint), points.timestamp(leftPoint));
                if (timestampOrder != 0) {
                    return timestampOrder;
                }
                ExponentialHistogram left = points.value(leftPoint, leftScratch);
                ExponentialHistogram right = points.value(rightPoint, rightScratch);
                return compareHistograms(left, right);
            }
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

        private static final class PointBuffer implements org.elasticsearch.core.Releasable {
            private final LongBuffer timestamps;
            private final ExponentialHistogramBuffer values;
            private final IntBuffer next;

            PointBuffer(BlockFactory blockFactory, CircuitBreaker breaker) {
                LongBuffer timestamps = null;
                ExponentialHistogramBuffer values = null;
                IntBuffer next = null;
                boolean success = false;
                try {
                    timestamps = new LongBuffer(breaker, PAGE_SIZE);
                    values = new ExponentialHistogramBuffer(blockFactory, PAGE_SIZE);
                    next = new IntBuffer(breaker, PAGE_SIZE);
                    success = true;
                } finally {
                    if (success == false) {
                        Releasables.close(timestamps, values, next);
                    }
                }
                this.timestamps = timestamps;
                this.values = values;
                this.next = next;
            }

            int append(long timestamp, ExponentialHistogram value, int nextPoint) {
                int id = timestamps.size();
                timestamps.ensureCapacity(id + 1);
                values.ensureCapacity(id + 1);
                next.ensureCapacity(id + 1);
                timestamps.append(timestamp);
                values.append(value);
                next.append(nextPoint);
                return id;
            }

            long timestamp(int point) {
                return timestamps.get(point);
            }

            ExponentialHistogram value(int point, ExponentialHistogramScratch scratch) {
                return values.get(point, scratch);
            }

            int next(int point) {
                return next.get(point);
            }

            @Override
            public void close() {
                Releasables.close(timestamps, values, next);
            }
        }
    }
}
