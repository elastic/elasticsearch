/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.aggregation.GroupingAggregatorState;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.IntVector;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.DriverContext;

@Aggregator(
    {
        @IntermediateState(name = "xVal", type = "DOUBLE"),
        @IntermediateState(name = "xDel", type = "DOUBLE"),
        @IntermediateState(name = "yVal", type = "DOUBLE"),
        @IntermediateState(name = "yDel", type = "DOUBLE") }
)
@GroupingAggregator
abstract class StExtentAggregator {
    public static StExtentState initSingle() {
        return new StExtentState();
    }

    public static GroupingStExtentState initGrouping() {
        return new GroupingStExtentState();
    }

    //
    // public static StExtentAggregator.ExtentState initGrouping(BigArrays bigArrays) {
    // return new StExtentAggregator.ExtentState(bigArrays);
    // }
    //
    public static void combine(StExtentState current, BytesRef wkb) {
        throw new AssertionError("TODO(gal)");
        // Point point = decode(wkb);
        // current.add(point.getX(), point.getY());
    }

    public static void combine(GroupingStExtentState current, int groupId, BytesRef wkb) {
        throw new AssertionError("TODO(gal)");
        // Point point = decode(wkb);
        // current.add(point.getX(), point.getY());
    }

    public static void combineIntermediate(StExtentState current, double minX, double minY, double maxX, double maxY) {
        throw new AssertionError("TODO(gal)");
    }

    public static void combineIntermediate(
        GroupingStExtentState current,
        int groupId,
        double minX,
        double minY,
        double maxX,
        double maxY
    ) {
        throw new AssertionError("TODO(gal)");
    }

    public static Block evaluateFinal(StExtentState state, DriverContext driverContext) {
        throw new AssertionError("TODO(gal)");
    }

    public static Block evaluateFinal(GroupingStExtentState state, IntVector selected, DriverContext driverContext) {
        throw new AssertionError("TODO(gal)");
    }
    //
    // public static void combine(StExtentAggregator.ExtentState current, int groupId, BytesRef wkb) {
    // Point point = decode(wkb);
    // current.add(point.getX(), 0d, point.getY(), 0d, 1, groupId);
    // }
    //
    // private static Point decode(BytesRef wkb) {
    // return (Point) WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
    // }

    public static void toIntermediate(Block[] blocks, int groupId, int offset, DriverContext driverContext) {
        throw new AssertionError("TODO(gal)");
    }

    public static void combineStates(StExtentState current, StExtentState inState) {
        throw new AssertionError("TODO(gal)");
    }

    public static void combineStates(
        GroupingStExtentState current,
        int groupId,
        GroupingStExtentState inState,
        int position
    ) {
        throw new AssertionError("TODO(gal)");
    }

    public static final class StExtentState implements AggregatorState {
        private BigArrays bigArrays;
        // 4-tuples of (minX, minY, maxX, maxY) per group ID.
        private DoubleArray extents;

        StExtentState() {
            this.bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
            this.extents = bigArrays.newDoubleArray(0, false);
        }

        @Override
        public void close() {
            throw new AssertionError("TODO(gal)");
        }

        void enableGroupIdTracking(SeenGroupIds ignore) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }

        public boolean hasValue(int position) {
            throw new AssertionError("TODO(gal)");
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            throw new AssertionError("TODO(gal)");
        }
    }

    public static final class GroupingStExtentState implements GroupingAggregatorState {
        private BigArrays bigArrays;
        // 4-tuples of (minX, minY, maxX, maxY) per group ID.
        private DoubleArray extents;

        GroupingStExtentState() {
            this(BigArrays.NON_RECYCLING_INSTANCE);
        }

        GroupingStExtentState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            this.extents = bigArrays.newDoubleArray(0, false);
        }

        @Override
        public void close() {
            throw new AssertionError("TODO(gal)");
        }

        void enableGroupIdTracking(SeenGroupIds ignore) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }

        public boolean hasValue(int position) {
            throw new AssertionError("TODO(gal)");
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            throw new AssertionError("TODO(gal)");
        }
    }

}
