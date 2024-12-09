/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.aggregation.GroupingAggregatorState;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.nio.ByteOrder;

/**
 * This aggregator calculates the centroid of a set of geo points or cartesian_points.
 * It is assumes that the points are encoded as longs.
 * This requires that the planner has planned that points are loaded from the index as doc-values.
 */
abstract class CentroidPointAggregator {

    public static void combine(CentroidState current, double xVal, double xDel, double yVal, double yDel, long count) {
        current.add(xVal, xDel, yVal, yDel, count);
    }

    public static void combineStates(CentroidState current, CentroidState state) {
        current.add(state);
    }

    public static void combineIntermediate(CentroidState state, double xIn, double dx, double yIn, double dy, long count) {
        if (count > 0) {
            combine(state, xIn, dx, yIn, dy, count);
        }
    }

    public static void evaluateIntermediate(CentroidState state, DriverContext driverContext, Block[] blocks, int offset) {
        assert blocks.length >= offset + 5;
        BlockFactory blockFactory = driverContext.blockFactory();
        blocks[offset + 0] = blockFactory.newConstantDoubleBlockWith(state.xSum.value(), 1);
        blocks[offset + 1] = blockFactory.newConstantDoubleBlockWith(state.xSum.delta(), 1);
        blocks[offset + 2] = blockFactory.newConstantDoubleBlockWith(state.ySum.value(), 1);
        blocks[offset + 3] = blockFactory.newConstantDoubleBlockWith(state.ySum.delta(), 1);
        blocks[offset + 4] = blockFactory.newConstantLongBlockWith(state.count, 1);
    }

    public static Block evaluateFinal(CentroidState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static void combineStates(GroupingCentroidState current, int groupId, GroupingCentroidState state, int statePosition) {
        if (state.hasValue(statePosition)) {
            current.add(
                state.xValues.get(statePosition),
                state.xDeltas.get(statePosition),
                state.yValues.get(statePosition),
                state.yDeltas.get(statePosition),
                state.counts.get(statePosition),
                groupId
            );
        }
    }

    public static void combineIntermediate(
        GroupingCentroidState current,
        int groupId,
        double xValue,
        double xDelta,
        double yValue,
        double yDelta,
        long count
    ) {
        if (count > 0) {
            current.add(xValue, xDelta, yValue, yDelta, count, groupId);
        }
    }

    public static void evaluateIntermediate(
        GroupingCentroidState state,
        Block[] blocks,
        int offset,
        IntVector selected,
        DriverContext driverContext
    ) {
        assert blocks.length >= offset + 5;
        try (
            var xValuesBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var xDeltaBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var yValuesBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var yDeltaBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var countsBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < state.xValues.size()) {
                    xValuesBuilder.appendDouble(state.xValues.get(group));
                    xDeltaBuilder.appendDouble(state.xDeltas.get(group));
                    yValuesBuilder.appendDouble(state.yValues.get(group));
                    yDeltaBuilder.appendDouble(state.yDeltas.get(group));
                    countsBuilder.appendLong(state.counts.get(group));
                } else {
                    xValuesBuilder.appendDouble(0);
                    xDeltaBuilder.appendDouble(0);
                    yValuesBuilder.appendDouble(0);
                    yDeltaBuilder.appendDouble(0);
                    countsBuilder.appendLong(0);
                }
            }
            blocks[offset + 0] = xValuesBuilder.build();
            blocks[offset + 1] = xDeltaBuilder.build();
            blocks[offset + 2] = yValuesBuilder.build();
            blocks[offset + 3] = yDeltaBuilder.build();
            blocks[offset + 4] = countsBuilder.build();
        }
    }

    public static Block evaluateFinal(GroupingCentroidState state, IntVector selected, DriverContext driverContext) {
        try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int si = selected.getInt(i);
                if (state.hasValue(si) && si < state.xValues.size()) {
                    BytesRef result = state.encodeCentroidResult(si);
                    builder.appendBytesRef(result);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private static BytesRef encode(double x, double y) {
        return new BytesRef(WellKnownBinary.toWKB(new Point(x, y), ByteOrder.LITTLE_ENDIAN));
    }

    static class CentroidState implements AggregatorState {
        protected final CompensatedSum xSum = new CompensatedSum(0, 0);
        protected final CompensatedSum ySum = new CompensatedSum(0, 0);
        protected long count = 0;

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            CentroidPointAggregator.evaluateIntermediate(this, driverContext, blocks, offset);
        }

        @Override
        public void close() {}

        public void count(long count) {
            this.count = count;
        }

        public void add(CentroidState other) {
            xSum.add(other.xSum.value(), other.xSum.delta());
            ySum.add(other.ySum.value(), other.ySum.delta());
            count += other.count;
        }

        public void add(double x, double y) {
            xSum.add(x);
            ySum.add(y);
            count++;
        }

        public void add(double x, double dx, double y, double dy, long count) {
            xSum.add(x, dx);
            ySum.add(y, dy);
            this.count += count;
        }

        protected Block toBlock(BlockFactory blockFactory) {
            if (count > 0) {
                double x = xSum.value() / count;
                double y = ySum.value() / count;
                return blockFactory.newConstantBytesRefBlockWith(encode(x, y), 1);
            } else {
                return blockFactory.newConstantNullBlock(1);
            }
        }
    }

    static class GroupingCentroidState implements GroupingAggregatorState {
        private final BigArrays bigArrays;

        DoubleArray xValues;
        DoubleArray xDeltas;
        DoubleArray yValues;
        DoubleArray yDeltas;

        LongArray counts;

        GroupingCentroidState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            boolean success = false;
            try {
                this.xValues = bigArrays.newDoubleArray(1);
                this.xDeltas = bigArrays.newDoubleArray(1);
                this.yValues = bigArrays.newDoubleArray(1);
                this.yDeltas = bigArrays.newDoubleArray(1);
                this.counts = bigArrays.newLongArray(1);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        void add(double x, double dx, double y, double dy, long count, int groupId) {
            ensureCapacity(groupId);

            // If the value is Inf or NaN, just add it to the running tally to "convert" to
            // Inf/NaN. This keeps the behavior bwc from before kahan summing
            if (Double.isFinite(x) == false || Double.isFinite(y) == false) {
                xValues.increment(groupId, x);
                yValues.increment(groupId, y);
                return;
            }

            addTo(xValues, xDeltas, groupId, x, dx);
            addTo(yValues, yDeltas, groupId, y, dy);
            counts.increment(groupId, count);
        }

        private static void addTo(DoubleArray values, DoubleArray deltas, int groupId, double valueToAdd, double deltaToAdd) {
            double value = values.get(groupId);
            if (Double.isFinite(value) == false) {
                // It isn't going to get any more infinite.
                return;
            }
            double delta = deltas.get(groupId);
            double correctedSum = valueToAdd + (delta + deltaToAdd);
            double updatedValue = value + correctedSum;
            deltas.set(groupId, correctedSum - (updatedValue - value));
            values.set(groupId, updatedValue);
        }

        boolean hasValue(int index) {
            return counts.get(index) > 0;
        }

        /** Needed for generated code that does null tracking, which we do not need because we use count */
        final void enableGroupIdTracking(SeenGroupIds ignore) {}

        private void ensureCapacity(int groupId) {
            if (groupId >= xValues.size()) {
                xValues = bigArrays.grow(xValues, groupId + 1);
                xDeltas = bigArrays.grow(xDeltas, groupId + 1);
                yValues = bigArrays.grow(yValues, groupId + 1);
                yDeltas = bigArrays.grow(yDeltas, groupId + 1);
                counts = bigArrays.grow(counts, groupId + 1);
            }
        }

        protected BytesRef encodeCentroidResult(int si) {
            long count = counts.get(si);
            double x = xValues.get(si) / count;
            double y = yValues.get(si) / count;
            return encode(x, y);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            CentroidPointAggregator.evaluateIntermediate(this, blocks, offset, selected, driverContext);
        }

        @Override
        public void close() {
            Releasables.close(xValues, xDeltas, yValues, yDeltas, counts);
        }
    }
}
