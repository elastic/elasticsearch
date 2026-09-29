/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.List;
// end generated imports

/**
 * {@link AggregatorFunctionSupplier} implementation for PromQL changes over double values.
 * This class is generated. Edit {@code X-ChangesAggregatorFunctionSupplier.java.st} instead.
 */
public final class ChangesDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return ChangesDoubleGroupingAggregatorFunction.intermediateStateDesc();
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        return new ChangesDoubleGroupingAggregatorFunction(channels, driverContext);
    }

    @Override
    public String describe() {
        return "changes of doubles";
    }
}

final class ChangesDoubleGroupingAggregatorFunction extends AbstractRateGroupingFunction implements GroupingAggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("timestamps", ElementType.LONG),
        new IntermediateStateDesc("values", ElementType.DOUBLE)
    );

    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    private final PointBuffer points;
    private ObjectArray<ReducedState> states;

    ChangesDoubleGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.bigArrays = driverContext.bigArrays();
        PointBuffer points = null;
        try {
            points = new PointBuffer(driverContext.breaker());
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
        DoubleBlock valuesBlock = page.getBlock(channels.get(0));
        LongBlock timestampsBlock = page.getBlock(channels.get(1));
        if (valuesBlock.areAllValuesNull() || timestampsBlock.areAllValuesNull()) {
            return null;
        }
        DoubleVector valuesVector = valuesBlock.asVector();
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

    private void addRawInput(int positionOffset, IntBlock groups, DoubleBlock values, LongBlock timestamps) {
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
                for (int t = timestampStart; t < timestampEnd; t++) {
                    long timestamp = timestamps.getLong(t);
                    for (int v = valueStart; v < valueEnd; v++) {
                        state.append(timestamp, values.getDouble(v));
                    }
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, DoubleBlock values, LongBlock timestamps) {
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
            for (int t = timestampStart; t < timestampEnd; t++) {
                long timestamp = timestamps.getLong(t);
                for (int v = valueStart; v < valueEnd; v++) {
                    state.append(timestamp, values.getDouble(v));
                }
            }
        }
    }

    private void addRawInput(int positionOffset, IntBlock groups, DoubleVector values, LongVector timestamps) {
        for (int p = 0; p < groups.getPositionCount(); p++) {
            if (groups.isNull(p)) {
                continue;
            }
            int valuePosition = positionOffset + p;
            double value = values.getDouble(valuePosition);
            long timestamp = timestamps.getLong(valuePosition);
            int groupStart = groups.getFirstValueIndex(p);
            int groupEnd = groupStart + groups.getValueCount(p);
            for (int g = groupStart; g < groupEnd; g++) {
                getOrInitializeState(groups.getInt(g)).append(timestamp, value);
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, DoubleVector values, LongVector timestamps) {
        for (int p = 0; p < groups.getPositionCount(); p++) {
            int valuePosition = positionOffset + p;
            getOrInitializeState(groups.getInt(p)).append(timestamps.getLong(valuePosition), values.getDouble(valuePosition));
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
        DoubleBlock values = page.getBlock(channels.get(1));
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
        DoubleBlock values = page.getBlock(channels.get(1));
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

    private void addIntermediatePosition(int groupId, LongBlock timestamps, DoubleBlock values, int position) {
        int count = timestamps.getValueCount(position);
        assert count == values.getValueCount(position) : "timestamps=" + timestamps + "; values=" + values + "; position=" + position;
        int firstTimestamp = timestamps.getFirstValueIndex(position);
        int firstValue = values.getFirstValueIndex(position);
        ReducedState state = getOrInitializeState(groupId);
        for (int i = 0; i < count; i++) {
            state.append(timestamps.getLong(firstTimestamp + i), values.getDouble(firstValue + i));
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
            DoubleBlock.Builder values = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)
        ) {
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
                        values.appendDouble(points.value(point));
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
        private int head = -1;
        private int count;

        void append(long timestamp, double value) {
            head = points.append(timestamp, value, head);
            count++;
        }

        long changes() {
            if (count < 2) {
                return 0;
            }
            // Pages and intermediate results for one series may have interleaved timestamps.
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
                    if (valuesEqual(points.value(sorted[i]), points.value(sorted[i - 1])) == false) {
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
            // Multiple values can share a timestamp. Order them by value so the result does not depend on page or merge order.
            return Double.compare(points.value(leftPoint), points.value(rightPoint));
        }

        private boolean valuesEqual(double left, double right) {
            return left == right || (Double.isNaN(left) && Double.isNaN(right));
        }
    }

    private static final class PointBuffer implements org.elasticsearch.core.Releasable {
        private final LongBuffer timestamps;
        private final DoubleBuffer values;
        private final IntBuffer next;

        PointBuffer(org.elasticsearch.common.breaker.CircuitBreaker breaker) {
            LongBuffer timestamps = null;
            DoubleBuffer values = null;
            IntBuffer next = null;
            boolean success = false;
            try {
                timestamps = new LongBuffer(breaker, PAGE_SIZE);
                values = new DoubleBuffer(breaker, PAGE_SIZE);
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

        int append(long timestamp, double value, int nextPoint) {
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

        double value(int point) {
            return values.get(point);
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
