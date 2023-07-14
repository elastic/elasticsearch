/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantDoubleVector;
import org.elasticsearch.compute.data.ConstantLongVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

@Aggregator(
    {
        @IntermediateState(name = "xVal", type = "DOUBLE"),
        @IntermediateState(name = "xDel", type = "DOUBLE"),
        @IntermediateState(name = "yVal", type = "DOUBLE"),
        @IntermediateState(name = "yDel", type = "DOUBLE"),
        @IntermediateState(name = "count", type = "LONG") }
)
@GroupingAggregator
class GeoCentroidGeoPointAggregator {
    public static CentroidState initSingle() {
        return new CentroidState();
    }

    public static void combine(CentroidState current, long v) {
        current.add(v);
    }

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

    public static void evaluateIntermediate(CentroidState state, Block[] blocks, int offset) {
        assert blocks.length >= offset + 5;
        blocks[offset + 0] = new ConstantDoubleVector(state.xSum.value(), 1).asBlock();
        blocks[offset + 1] = new ConstantDoubleVector(state.xSum.delta(), 1).asBlock();
        blocks[offset + 2] = new ConstantDoubleVector(state.ySum.value(), 1).asBlock();
        blocks[offset + 3] = new ConstantDoubleVector(state.ySum.delta(), 1).asBlock();
        blocks[offset + 4] = new ConstantLongVector(state.count, 1).asBlock();
    }

    public static Block evaluateFinal(CentroidState state) {
        long result = encodeCentroidResult(state.xSum.value(), state.ySum.value(), state.count);
        return LongBlock.newConstantBlockWith(result, 1);
    }

    private static long encodeCentroidResult(double xSum, double ySum, long count) {
        double x = xSum / count;
        double y = ySum / count;
        return (long) GeoEncodingUtils.encodeLatitude(y) << 32 | (long) GeoEncodingUtils.encodeLongitude(x);
    }

    public static GroupingCentroidState initGrouping(BigArrays bigArrays) {
        return new GroupingCentroidState(bigArrays);
    }

    public static void combine(GroupingCentroidState current, int groupId, long encoded) {
        current.add(encoded, groupId);
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

    public static void evaluateIntermediate(GroupingCentroidState state, Block[] blocks, int offset, IntVector selected) {
        assert blocks.length >= offset + 5;
        var xValuesBuilder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        var xDeltaBuilder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        var yValuesBuilder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        var yDeltaBuilder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        var countsBuilder = LongBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            xValuesBuilder.appendDouble(state.xValues.get(group));
            xDeltaBuilder.appendDouble(state.xDeltas.get(group));
            yValuesBuilder.appendDouble(state.yValues.get(group));
            yDeltaBuilder.appendDouble(state.yDeltas.get(group));
            countsBuilder.appendLong(state.counts.get(group));
        }
        blocks[offset + 0] = xValuesBuilder.build();
        blocks[offset + 1] = xDeltaBuilder.build();
        blocks[offset + 2] = yValuesBuilder.build();
        blocks[offset + 3] = yDeltaBuilder.build();
        blocks[offset + 4] = countsBuilder.build();
    }

    public static Block evaluateFinal(GroupingCentroidState state, IntVector selected) {
        LongBlock.Builder builder = LongBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            if (state.hasValue(i)) {
                double x = state.xValues.get(selected.getInt(i));
                double y = state.yValues.get(selected.getInt(i));
                long count = state.counts.get(selected.getInt(i));
                long result = encodeCentroidResult(x, y, count);
                builder.appendLong(result);
            } else {
                builder.appendNull();
            }
        }
        return builder.build();
    }

    static class CentroidState implements AggregatorState {
        private final CompensatedSum xSum = new CompensatedSum(0, 0);
        private final CompensatedSum ySum = new CompensatedSum(0, 0);
        private long count = 0;

        @Override
        public void toIntermediate(Block[] blocks, int offset) {
            GeoCentroidGeoPointAggregator.evaluateIntermediate(this, blocks, offset);
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

        public void add(long v) {
            double y = GeoEncodingUtils.decodeLatitude((int) (v >>> 32));
            double x = GeoEncodingUtils.decodeLongitude((int) v);
            add(x, y);
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

        public void add(long encoded, int groupId) {
            double y = GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32));
            double x = GeoEncodingUtils.decodeLongitude((int) encoded);
            add(x, 0d, y, 0d, 1, groupId);
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

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected) {
            GeoCentroidGeoPointAggregator.evaluateIntermediate(this, blocks, offset, selected);
        }

        @Override
        public void close() {
            Releasables.close(xValues, xDeltas, yValues, yDeltas, counts);
        }
    }
}
