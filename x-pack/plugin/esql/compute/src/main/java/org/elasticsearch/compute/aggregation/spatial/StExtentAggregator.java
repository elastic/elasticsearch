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
import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.aggregation.GroupingAggregatorState;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.WellKnownBinary;

import java.nio.ByteOrder;
import java.util.Optional;

abstract class StExtentAggregator {
    public static StExtentState initSingle() {
        return new StExtentState();
    }

    public static GroupingStExtentState initGrouping() {
        return new GroupingStExtentState();
    }

    public static void combineIntermediate(StExtentState current, BytesRef wkb) {
        current.extent = Optional.of(combineIntermediate(current.extent, SpatialAggregationUtils.decodeRectangle(wkb)));
    }

    public static void combineIntermediate(GroupingStExtentState current, int groupId, BytesRef wkb) {
        current.add(groupId, SpatialAggregationUtils.decodeRectangle(wkb));
    }

    private static Rectangle combineIntermediate(Optional<Rectangle> r1, Rectangle r2) {
        return r1.map(
            oldExtent -> new Rectangle(
                Math.min(oldExtent.getMinX(), r2.getMinX()),
                Math.max(oldExtent.getMaxX(), r2.getMaxX()),
                Math.max(oldExtent.getMaxY(), r2.getMaxY()),
                Math.min(oldExtent.getMinY(), r2.getMinY())
            )
        ).orElseGet(() -> new Rectangle(r2.getMinX(), r2.getMaxX(), r2.getMaxY(), r2.getMinY()));
    }

    public static Block evaluateFinal(StExtentState state, DriverContext driverContext) {
        return state.toBlock(driverContext);
    }

    public static Block evaluateFinal(GroupingStExtentState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(selected, driverContext);
    }

    public static void combineStates(GroupingStExtentState current, int groupId, GroupingStExtentState inState, int inPosition) {
        inState.getExtent(inPosition).ifPresent(extent -> current.add(groupId, extent));
    }

    public static final class StExtentState implements AggregatorState {
        private Optional<Rectangle> extent = Optional.empty();

        @Override
        public void close() {}

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset;
            blocks[offset] = toBlock(driverContext);
        }

        public void add(Geometry geo) {
            if (SpatialEnvelopeVisitor.visit(geo).orElse(null) instanceof Rectangle newExtent) {
                this.extent = Optional.of(combineIntermediate(this.extent, newExtent));
            }
        }

        public Block toBlock(DriverContext driverContext) {
            return extent.<Block>map(
                r -> (driverContext.blockFactory()
                    .newConstantBytesRefBlockWith(new BytesRef(WellKnownBinary.toWKB(r, ByteOrder.LITTLE_ENDIAN)), 1))
            ).orElseGet(() -> driverContext.blockFactory().newConstantNullBlock(1));
        }
    }

    public static final class GroupingStExtentState implements GroupingAggregatorState {
        private final BigArrays bigArrays;
        // 4-tuples of (minX, maxX, maxY, minY) per group ID.
        private DoubleArray extents;

        GroupingStExtentState() {
            this(BigArrays.NON_RECYCLING_INSTANCE);
        }

        GroupingStExtentState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            this.extents = bigArrays.newDoubleArray(0, false);
            this.extents.fill(0, extents.size(), Double.NaN);
        }

        @Override
        public void close() {
            extents.close();
        }

        void enableGroupIdTracking(SeenGroupIds ignore) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset;
            blocks[offset] = toBlock(selected, driverContext);
        }

        public void add(int groupId, Geometry geometry) {
            ensureCapacity(groupId);
            SpatialEnvelopeVisitor.visit(geometry).ifPresent(r -> setExtent(groupId, combineIntermediate(getExtent(groupId), r)));
        }

        private Optional<Rectangle> getExtent(int groupId) {
            long offset = (long) groupId * PARAMETER_COUNT;
            return extents.size() >= offset + PARAMETER_COUNT && Double.isFinite(extents.get(offset))
                ? Optional.of(new Rectangle(extents.get(offset), extents.get(offset + 1), extents.get(offset + 2), extents.get(offset + 3)))
                : Optional.empty();
        }

        private void setExtent(int groupId, Rectangle r) {
            assert extents.size() >= (groupId + 1L) * PARAMETER_COUNT;
            long offset = (long) groupId * PARAMETER_COUNT;
            extents.set(offset, r.getMinX());
            extents.set(offset + 1, r.getMaxX());
            extents.set(offset + 2, r.getMaxY());
            extents.set(offset + 3, r.getMinY());
        }

        private void ensureCapacity(int groupId) {
            long offset = (long) groupId * PARAMETER_COUNT;
            long requiredCapacity = offset + PARAMETER_COUNT;
            if (extents.size() < requiredCapacity) {
                var oldSize = extents.size();
                extents = bigArrays.grow(extents, requiredCapacity);
                extents.fill(oldSize, extents.size(), Double.NaN);
            }
        }

        public Block toBlock(IntVector selected, DriverContext driverContext) {
            try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int si = selected.getInt(i);
                    if (getExtent(si).orElse(null) instanceof Rectangle r) {
                        builder.appendBytesRef(new BytesRef(WellKnownBinary.toWKB(r, ByteOrder.LITTLE_ENDIAN)));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }
    }

    private static final int PARAMETER_COUNT = 4;
}
