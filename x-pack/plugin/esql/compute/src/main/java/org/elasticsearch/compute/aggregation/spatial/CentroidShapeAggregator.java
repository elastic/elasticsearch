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
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
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
import org.elasticsearch.lucene.spatial.DimensionalShapeType;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.nio.ByteOrder;

/**
 * This aggregator calculates the centroid of a set of shapes (geo_shape or cartesian_shape).
 * Unlike the point-based centroid, this needs to track the dimensional shape type and weight
 * to correctly handle polygons (weighted by area), lines (weighted by length), and points (weight 1).
 * When shapes of different dimensional types are combined, only the highest dimensional type contributes.
 */
abstract class CentroidShapeAggregator {
    public static ShapeCentroidState initSingle() {
        return new ShapeCentroidState();
    }

    public static GroupingShapeCentroidState initGrouping(BigArrays bigArrays) {
        return new GroupingShapeCentroidState(bigArrays);
    }

    public static void combine(
        ShapeCentroidState current,
        double xVal,
        double xDel,
        double yVal,
        double yDel,
        double weight,
        int shapeType
    ) {
        current.add(xVal, xDel, yVal, yDel, weight, DimensionalShapeType.fromOrdinalByte((byte) shapeType));
    }

    public static void combineIntermediate(
        ShapeCentroidState state,
        double xIn,
        double dx,
        double yIn,
        double dy,
        double weight,
        int shapeType
    ) {
        if (weight > 0) {
            combine(state, xIn, dx, yIn, dy, weight, shapeType);
        }
    }

    public static void evaluateIntermediate(ShapeCentroidState state, DriverContext driverContext, Block[] blocks, int offset) {
        assert blocks.length >= offset + 6;
        BlockFactory blockFactory = driverContext.blockFactory();
        blocks[offset + 0] = blockFactory.newConstantDoubleBlockWith(state.xSum.value(), 1);
        blocks[offset + 1] = blockFactory.newConstantDoubleBlockWith(state.xSum.delta(), 1);
        blocks[offset + 2] = blockFactory.newConstantDoubleBlockWith(state.ySum.value(), 1);
        blocks[offset + 3] = blockFactory.newConstantDoubleBlockWith(state.ySum.delta(), 1);
        blocks[offset + 4] = blockFactory.newConstantDoubleBlockWith(state.weight, 1);
        blocks[offset + 5] = blockFactory.newConstantIntBlockWith(state.shapeType.ordinal(), 1);
    }

    public static Block evaluateFinal(ShapeCentroidState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static void combineIntermediate(
        GroupingShapeCentroidState current,
        int groupId,
        double xValue,
        double xDelta,
        double yValue,
        double yDelta,
        double weight,
        int shapeType
    ) {
        if (weight > 0) {
            current.add(xValue, xDelta, yValue, yDelta, weight, DimensionalShapeType.fromOrdinalByte((byte) shapeType), groupId);
        }
    }

    public static void evaluateIntermediate(
        GroupingShapeCentroidState state,
        Block[] blocks,
        int offset,
        IntVector selected,
        DriverContext driverContext
    ) {
        assert blocks.length >= offset + 6;
        try (
            var xValuesBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var xDeltaBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var yValuesBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var yDeltaBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var weightsBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var shapeTypesBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount())
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < state.xValues.size()) {
                    xValuesBuilder.appendDouble(state.xValues.get(group));
                    xDeltaBuilder.appendDouble(state.xDeltas.get(group));
                    yValuesBuilder.appendDouble(state.yValues.get(group));
                    yDeltaBuilder.appendDouble(state.yDeltas.get(group));
                    weightsBuilder.appendDouble(state.weights.get(group));
                    shapeTypesBuilder.appendInt(state.shapeTypes.get(group));
                } else {
                    xValuesBuilder.appendDouble(0);
                    xDeltaBuilder.appendDouble(0);
                    yValuesBuilder.appendDouble(0);
                    yDeltaBuilder.appendDouble(0);
                    weightsBuilder.appendDouble(0);
                    shapeTypesBuilder.appendInt(DimensionalShapeType.POINT.ordinal());
                }
            }
            blocks[offset + 0] = xValuesBuilder.build();
            blocks[offset + 1] = xDeltaBuilder.build();
            blocks[offset + 2] = yValuesBuilder.build();
            blocks[offset + 3] = yDeltaBuilder.build();
            blocks[offset + 4] = weightsBuilder.build();
            blocks[offset + 5] = shapeTypesBuilder.build();
        }
    }

    public static Block evaluateFinal(GroupingShapeCentroidState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        try (BytesRefBlock.Builder builder = ctx.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
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

    static class ShapeCentroidState implements AggregatorState {
        protected final CompensatedSum xSum = new CompensatedSum(0, 0);
        protected final CompensatedSum ySum = new CompensatedSum(0, 0);
        protected double weight = 0;
        protected DimensionalShapeType shapeType = DimensionalShapeType.POINT;

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            CentroidShapeAggregator.evaluateIntermediate(this, driverContext, blocks, offset);
        }

        @Override
        public void close() {}

        /**
         * Add a coordinate with weight and shape type, respecting dimensional priority.
         * When mixing shapes of different dimensional types, only the highest dimensional type contributes.
         */
        public void add(double x, double dx, double y, double dy, double weight, DimensionalShapeType shapeType) {
            if (Double.isFinite(x) == false || Double.isFinite(y) == false || Double.isFinite(weight) == false || weight <= 0) {
                return;
            }
            int cmp = shapeType.compareTo(this.shapeType);
            if (cmp == 0) {
                // Same dimension - add to running totals
                xSum.add(x, dx);
                ySum.add(y, dy);
                this.weight += weight;
            } else if (cmp > 0) {
                // Higher dimension - reset and start fresh
                xSum.reset(x, dx);
                ySum.reset(y, dy);
                this.weight = weight;
                this.shapeType = shapeType;
            }
            // If cmp < 0, the incoming shape is lower dimension, so we ignore it
        }

        protected Block toBlock(BlockFactory blockFactory) {
            if (weight > 0) {
                double x = xSum.value() / weight;
                double y = ySum.value() / weight;
                return blockFactory.newConstantBytesRefBlockWith(encode(x, y), 1);
            } else {
                return blockFactory.newConstantNullBlock(1);
            }
        }
    }

    static class GroupingShapeCentroidState implements GroupingAggregatorState {
        private final BigArrays bigArrays;

        private DoubleArray xValues;
        private DoubleArray xDeltas;
        private DoubleArray yValues;
        private DoubleArray yDeltas;
        private DoubleArray weights;
        private IntArray shapeTypes;

        GroupingShapeCentroidState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            boolean success = false;
            try {
                this.xValues = bigArrays.newDoubleArray(1, true);
                this.xDeltas = bigArrays.newDoubleArray(1, true);
                this.yValues = bigArrays.newDoubleArray(1, true);
                this.yDeltas = bigArrays.newDoubleArray(1, true);
                this.weights = bigArrays.newDoubleArray(1, true);
                this.shapeTypes = bigArrays.newIntArray(1, true);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        void add(double x, double dx, double y, double dy, double weight, DimensionalShapeType shapeType, int groupId) {
            if (Double.isFinite(x) == false || Double.isFinite(y) == false || Double.isFinite(weight) == false || weight <= 0) {
                return;
            }
            ensureCapacity(groupId);

            double currentWeight = weights.get(groupId);
            if (currentWeight == 0) {
                // First value for this group - initialize
                xValues.set(groupId, x);
                xDeltas.set(groupId, dx);
                yValues.set(groupId, y);
                yDeltas.set(groupId, dy);
                weights.set(groupId, weight);
                shapeTypes.set(groupId, shapeType.ordinal());
                return;
            }

            int currentShapeType = shapeTypes.get(groupId);
            int newShapeType = shapeType.ordinal();
            int cmp = Integer.compare(newShapeType, currentShapeType);

            if (cmp == 0) {
                // Same dimension - add to running totals
                addTo(xValues, xDeltas, groupId, x, dx);
                addTo(yValues, yDeltas, groupId, y, dy);
                weights.increment(groupId, weight);
            } else if (cmp > 0) {
                // Higher dimension - reset and start fresh
                xValues.set(groupId, x);
                xDeltas.set(groupId, dx);
                yValues.set(groupId, y);
                yDeltas.set(groupId, dy);
                weights.set(groupId, weight);
                shapeTypes.set(groupId, newShapeType);
            }
            // If cmp < 0, the incoming shape is lower dimension, so we ignore it
        }

        private static void addTo(DoubleArray values, DoubleArray deltas, int groupId, double valueToAdd, double deltaToAdd) {
            double value = values.get(groupId);
            if (Double.isFinite(value) == false) {
                return;
            }
            double delta = deltas.get(groupId);
            double correctedSum = valueToAdd + (delta + deltaToAdd);
            double updatedValue = value + correctedSum;
            deltas.set(groupId, correctedSum - (updatedValue - value));
            values.set(groupId, updatedValue);
        }

        boolean hasValue(int index) {
            return weights.get(index) > 0;
        }

        @Override
        public final void enableGroupIdTracking(SeenGroupIds ignore) {}

        private void ensureCapacity(int groupId) {
            long requiredSize = groupId + 1;
            if (xValues.size() < requiredSize) {
                xValues = bigArrays.grow(xValues, requiredSize);
                xDeltas = bigArrays.grow(xDeltas, requiredSize);
                yValues = bigArrays.grow(yValues, requiredSize);
                yDeltas = bigArrays.grow(yDeltas, requiredSize);
                weights = bigArrays.grow(weights, requiredSize);
            }
            // Since shape types are stored as ints, we grow them in different increments
            if (shapeTypes.size() < requiredSize) {
                shapeTypes = bigArrays.grow(shapeTypes, requiredSize);
            }
        }

        protected BytesRef encodeCentroidResult(int si) {
            double weight = weights.get(si);
            double x = xValues.get(si) / weight;
            double y = yValues.get(si) / weight;
            return encode(x, y);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            CentroidShapeAggregator.evaluateIntermediate(this, blocks, offset, selected, driverContext);
        }

        @Override
        public void close() {
            Releasables.close(xValues, xDeltas, yValues, yDeltas, weights, shapeTypes);
        }
    }
}
