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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Streams;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownBinary;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

@Aggregator(
    {
        @IntermediateState(name = "minX", type = "DOUBLE"),
        @IntermediateState(name = "maxX", type = "DOUBLE"),
        @IntermediateState(name = "minY", type = "DOUBLE"),
        @IntermediateState(name = "maxY", type = "DOUBLE") }
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

    public static void combineIntermediate(StExtentState current, double minX, double maxX, double maxY, double minY) {
        current.extent = Optional.of(combineIntermediate(current.extent, minX, maxX, maxY, minY));
    }

    public static void combineIntermediate(GroupingStExtentState current, int groupId, double minX, double maxX, double maxY, double minY) {
        current.setExtent(groupId, combineIntermediate(current.getExtent(groupId), minX, maxX, maxY, minY));
    }

    private static Rectangle combineIntermediate(Optional<Rectangle> r1, Rectangle r2) {
        return combineIntermediate(r1, r2.getMinX(), r2.getMaxX(), r2.getMaxY(), r2.getMinY());
    }

    private static Rectangle combineIntermediate(Optional<Rectangle> r, double minX, double maxX, double maxY, double minY) {
        return r.map(
            oldExtent -> new Rectangle(
                Math.min(oldExtent.getMinX(), minX),
                Math.max(oldExtent.getMaxX(), maxX),
                Math.max(oldExtent.getMaxY(), maxY),
                Math.min(oldExtent.getMinY(), minY)
            )
        ).orElseGet(() -> new Rectangle(minX, maxX, maxY, minY));
    }

    public static Block evaluateFinal(StExtentState state, DriverContext driverContext) {
        return state.extent.<Block>map(
            r -> (driverContext.blockFactory()
                .newConstantBytesRefBlockWith(new BytesRef(WellKnownBinary.toWKB(r, ByteOrder.LITTLE_ENDIAN)), 1))
        ).orElseGet(() -> driverContext.blockFactory().newConstantNullBlock(1));
    }

    public static Block evaluateFinal(GroupingStExtentState state, IntVector selected, DriverContext driverContext) {
        try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int si = selected.getInt(i);
                if (state.getExtent(si).orElse(null) instanceof Rectangle r) {
                    builder.appendBytesRef(new BytesRef(WellKnownBinary.toWKB(r, ByteOrder.LITTLE_ENDIAN)));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
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

    public static void combineStates(GroupingStExtentState current, int groupId, GroupingStExtentState inState, int position) {
        throw new AssertionError("TODO(gal)");
    }

    public static final class StExtentState implements AggregatorState {
        Optional<Rectangle> extent = Optional.empty();

        @Override
        public void close() {}

        void enableGroupIdTracking(SeenGroupIds ignore) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }

        public boolean hasValue(int position) {
            throw new AssertionError("TODO(gal)");
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + PARAMETER_COUNT;
            var blockFactory = driverContext.blockFactory();
            blocks[offset + 0] = blockFactory.newConstantDoubleBlockWith(extent.map(Rectangle::getMinX).orElse(POSITIVE_INFINITY), 1);
            blocks[offset + 1] = blockFactory.newConstantDoubleBlockWith(extent.map(Rectangle::getMaxX).orElse(NEGATIVE_INFINITY), 1);
            blocks[offset + 2] = blockFactory.newConstantDoubleBlockWith(extent.map(Rectangle::getMaxY).orElse(NEGATIVE_INFINITY), 1);
            blocks[offset + 3] = blockFactory.newConstantDoubleBlockWith(extent.map(Rectangle::getMinY).orElse(POSITIVE_INFINITY), 1);
        }

        public void add(Geometry geo) {
            if (geo.visit(new ExtentVisitor()).orElse(null) instanceof Rectangle newExtent) {
                combineIntermediate(this, newExtent.getMinX(), newExtent.getMaxX(), newExtent.getMaxY(), newExtent.getMinY());
            }
        }
    }

    public static final class GroupingStExtentState implements GroupingAggregatorState {
        private BigArrays bigArrays;
        // 4-tuples of (minX, maxX, maxY, minY) per group ID.
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
            extents.close();
        }

        void enableGroupIdTracking(SeenGroupIds ignore) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }

        public boolean hasValue(int position) {
            var offset = (long) position * PARAMETER_COUNT;
            return extents.size() > offset && Double.isFinite(extents.get(offset));
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            throw new AssertionError("TODO(gal)");
        }

        public void add(int groupId, Geometry geometry) {
            ensureCapacity(groupId);

            SpatialEnvelopeVisitor.visit(geometry).ifPresent(r -> setExtent(groupId, combineIntermediate(getExtent(groupId), r)));
        }

        private Optional<Rectangle> getExtent(int groupId) {
            assert extents.size() >= (groupId + 1L) * PARAMETER_COUNT;
            long offset = (long) groupId * PARAMETER_COUNT;
            return Double.isFinite(extents.get(offset))
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
                extents = bigArrays.grow(extents, requiredCapacity);
                extents.fill(offset, extents.size(), Double.NaN);
            }
        }
    }

    public static Rectangle combine(Rectangle r1, Rectangle r2) {
        return new Rectangle(
            Math.min(r1.getMinX(), r2.getMinX()),
            Math.max(r1.getMaxX(), r2.getMaxX()),
            Math.max(r1.getMaxY(), r2.getMaxY()),
            Math.min(r1.getMinY(), r2.getMinY())
        );
    }

    private static class ExtentVisitor implements GeometryVisitor<Optional<Rectangle>, RuntimeException> {
        @Override
        public Optional<Rectangle> visit(Circle circle) throws RuntimeException {
            return Optional.of(
                new Rectangle(
                    circle.getX() + circle.getRadiusMeters(),
                    circle.getX() - circle.getRadiusMeters(),
                    circle.getY() + circle.getRadiusMeters(),
                    circle.getY() - circle.getRadiusMeters()
                )
            );
        }

        @Override
        public Optional<Rectangle> visit(GeometryCollection<?> collection) throws RuntimeException {
            return StreamSupport.stream(collection.spliterator(), false)
                .map(g -> g.visit(this))
                .flatMap(Optional::stream)
                .reduce(StExtentAggregator::combine);
        }

        @Override
        public Optional<Rectangle> visit(Line line) throws RuntimeException {
            double minX = POSITIVE_INFINITY;
            double maxX = Double.NEGATIVE_INFINITY;
            double maxY = Double.NEGATIVE_INFINITY;
            double minY = POSITIVE_INFINITY;
            for (var x : line.getX()) {
                minX = Math.min(minX, x);
                maxX = Math.max(maxX, x);
            }
            for (var y : line.getY()) {
                minY = Math.min(minY, y);
                maxY = Math.max(maxY, y);
            }
            return Optional.of(new Rectangle(minX, maxX, maxY, minY));
        }

        @Override
        public Optional<Rectangle> visit(LinearRing ring) throws RuntimeException {
            throw new AssertionError("TODO(gal)");
        }

        @Override
        public Optional<Rectangle> visit(MultiLine multiLine) throws RuntimeException {
            throw new AssertionError("TODO(gal)");
        }

        @Override
        public Optional<Rectangle> visit(MultiPoint multiPoint) throws RuntimeException {
            throw new AssertionError("TODO(gal)");
        }

        @Override
        public Optional<Rectangle> visit(MultiPolygon multiPolygon) throws RuntimeException {
            throw new AssertionError("TODO(gal)");
        }

        @Override
        public Optional<Rectangle> visit(Point point) throws RuntimeException {
            return Optional.of(new Rectangle(point.getX(), point.getX(), point.getY(), point.getY()));
        }

        @Override
        public Optional<Rectangle> visit(Polygon polygon) throws RuntimeException {
            throw new AssertionError("TODO(gal)");
        }

        @Override
        public Optional<Rectangle> visit(Rectangle rectangle) throws RuntimeException {
            throw new AssertionError("TODO(gal)");
        }
    }

    private static final int PARAMETER_COUNT = 4;
}
