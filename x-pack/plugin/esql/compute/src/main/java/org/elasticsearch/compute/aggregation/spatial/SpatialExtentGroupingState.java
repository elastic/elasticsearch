/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.AbstractArrayState;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;

import java.nio.ByteOrder;

final class SpatialExtentGroupingState extends AbstractArrayState {
    private final PointType pointType;
    private IntArray minXs;
    private IntArray maxXs;
    private IntArray maxYs;
    private IntArray minYs;

    SpatialExtentGroupingState(PointType pointType) {
        this(pointType, BigArrays.NON_RECYCLING_INSTANCE);
    }

    SpatialExtentGroupingState(PointType pointType, BigArrays bigArrays) {
        super(bigArrays);
        this.pointType = pointType;
        this.minXs = bigArrays.newIntArray(0, false);
        this.maxXs = bigArrays.newIntArray(0, false);
        this.maxYs = bigArrays.newIntArray(0, false);
        this.minYs = bigArrays.newIntArray(0, false);
        enableGroupIdTracking(new SeenGroupIds.Empty());
    }

    @Override
    public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        assert blocks.length >= offset;
        try (
            var minXsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var maxXsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var maxYsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var minYsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    minXsBuilder.appendInt(minXs.get(group));
                    maxXsBuilder.appendInt(maxXs.get(group));
                    maxYsBuilder.appendInt(maxYs.get(group));
                    minYsBuilder.appendInt(minYs.get(group));
                } else {
                    // TODO: Should we add Nulls here instead?
                    minXsBuilder.appendInt(Integer.MAX_VALUE);
                    maxXsBuilder.appendInt(Integer.MIN_VALUE);
                    maxYsBuilder.appendInt(Integer.MIN_VALUE);
                    minYsBuilder.appendInt(Integer.MAX_VALUE);
                }
            }
            blocks[offset + 0] = minXsBuilder.build();
            blocks[offset + 1] = maxXsBuilder.build();
            blocks[offset + 2] = maxYsBuilder.build();
            blocks[offset + 3] = minYsBuilder.build();
        }
    }

    /**
     * This method is used when extents are extracted from the doc-values field by the {@link GeometryDocValueReader}.
     * This optimization is enabled when the field has doc-values and is only used in the ST_EXTENT aggregation.
     */
    public void add(int groupId, int p, IntBlock values) {
        int count = values.getValueCount(p);
        if (count == 6) {
            // Values are stored according to the order defined in the Extent class
            int i = values.getFirstValueIndex(p);
            int top = values.getInt(i++);
            int bottom = values.getInt(i++);
            int negLeft = values.getInt(i++);
            int negRight = values.getInt(i++);
            int posLeft = values.getInt(i++);
            int posRight = values.getInt(i);
            add(groupId, Math.min(negLeft, posLeft), Math.max(negRight, posRight), top, bottom);
        } else if (count == 4) {
            // Values are stored according to the order defined in the Rectangle class
            int i = values.getFirstValueIndex(p);
            int minX = values.getInt(i++);
            int maxX = values.getInt(i++);
            int maxY = values.getInt(i++);
            int minY = values.getInt(i);
            add(groupId, minX, maxX, maxY, minY);
        } else {
            throw new IllegalArgumentException("Expected 4 or 6 values, got " + count);
        }
    }

    public void add(int groupId, Geometry geometry) {
        ensureCapacity(groupId);
        pointType.computeEnvelope(geometry)
            .ifPresent(
                r -> add(
                    groupId,
                    pointType.encoder().encodeX(r.getMinX()),
                    pointType.encoder().encodeX(r.getMaxX()),
                    pointType.encoder().encodeY(r.getMaxY()),
                    pointType.encoder().encodeY(r.getMinY())
                )
            );
    }

    /**
     * This method is used when the field is a geo_point or cartesian_point and is loaded from doc-values.
     * This optimization is enabled when the field has doc-values and is only used in a spatial aggregation.
     */
    public void add(int groupId, long encoded) {
        int x = pointType.extractX(encoded);
        int y = pointType.extractY(encoded);
        add(groupId, x, x, y, y);
    }

    public void add(int groupId, int minX, int maxX, int maxY, int minY) {
        ensureCapacity(groupId);
        if (hasValue(groupId)) {
            minXs.set(groupId, Math.min(minXs.get(groupId), minX));
            maxXs.set(groupId, Math.max(maxXs.get(groupId), maxX));
            maxYs.set(groupId, Math.max(maxYs.get(groupId), maxY));
            minYs.set(groupId, Math.min(minYs.get(groupId), minY));
        } else {
            minXs.set(groupId, minX);
            maxXs.set(groupId, maxX);
            maxYs.set(groupId, maxY);
            minYs.set(groupId, minY);
        }
        trackGroupId(groupId);
    }

    private void ensureCapacity(int groupId) {
        long requiredSize = groupId + 1;
        if (minXs.size() < requiredSize) {
            assert minXs.size() == maxXs.size() && minXs.size() == maxYs.size() && minXs.size() == minYs.size();
            minXs = bigArrays.grow(minXs, requiredSize);
            maxXs = bigArrays.grow(maxXs, requiredSize);
            maxYs = bigArrays.grow(maxYs, requiredSize);
            minYs = bigArrays.grow(minYs, requiredSize);
        }
    }

    public Block toBlock(IntVector selected, DriverContext driverContext) {
        try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int si = selected.getInt(i);
                if (hasValue(si) == false) {
                    builder.appendNull();
                    continue;
                }
                int minX = minXs.get(si);
                int maxX = maxXs.get(si);
                int maxY = maxYs.get(si);
                int minY = minYs.get(si);

                boolean hasInfinity = minX == Integer.MAX_VALUE
                    || maxX == Integer.MIN_VALUE
                    || minY == Integer.MAX_VALUE
                    || maxY == Integer.MIN_VALUE;
                if (hasInfinity) {
                    builder.appendNull();
                    continue;
                }

                BytesRef bytes = new BytesRef(
                    WellKnownBinary.toWKB(
                        new Rectangle(
                            pointType.encoder().decodeX(minX),
                            pointType.encoder().decodeX(maxX),
                            pointType.encoder().decodeY(maxY),
                            pointType.encoder().decodeY(minY)
                        ),
                        ByteOrder.LITTLE_ENDIAN
                    )
                );

                builder.appendBytesRef(bytes);
            }
            return builder.build();
        }
    }
}
