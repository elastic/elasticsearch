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
import org.elasticsearch.compute.aggregation.GroupingAggregatorState;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;

import java.nio.ByteOrder;

import static org.elasticsearch.compute.aggregation.spatial.SpatialExtentStateWrappedLongitudeState.asRectangle;

final class SpatialExtentGroupingStateWrappedLongitudeState extends AbstractArrayState implements GroupingAggregatorState {
    // Only geo points support longitude wrapping.
    private static final PointType POINT_TYPE = PointType.GEO;
    private IntArray maxYs;
    private IntArray minYs;
    private IntArray minNegXs;
    private IntArray maxNegXs;
    private IntArray minPosXs;
    private IntArray maxPosXs;

    private final SpatialEnvelopeVisitor.GeoPointVisitor geoPointVisitor;

    SpatialExtentGroupingStateWrappedLongitudeState() {
        this(BigArrays.NON_RECYCLING_INSTANCE);
    }

    SpatialExtentGroupingStateWrappedLongitudeState(BigArrays bigArrays) {
        super(bigArrays);
        this.minNegXs = bigArrays.newIntArray(0, false);
        this.minPosXs = bigArrays.newIntArray(0, false);
        this.maxNegXs = bigArrays.newIntArray(0, false);
        this.maxPosXs = bigArrays.newIntArray(0, false);
        this.maxYs = bigArrays.newIntArray(0, false);
        this.minYs = bigArrays.newIntArray(0, false);
        enableGroupIdTracking(new SeenGroupIds.Empty());
        this.geoPointVisitor = new SpatialEnvelopeVisitor.GeoPointVisitor(SpatialEnvelopeVisitor.WrapLongitude.WRAP);
    }

    @Override
    public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        assert blocks.length >= offset;
        try (
            var minNegXsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var minPosXsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var maxNegXsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var maxPosXsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var maxYsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var minYsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                assert hasValue(group);
                minNegXsBuilder.appendInt(minNegXs.get(group));
                minPosXsBuilder.appendInt(minPosXs.get(group));
                maxNegXsBuilder.appendInt(maxNegXs.get(group));
                maxPosXsBuilder.appendInt(maxPosXs.get(group));
                maxYsBuilder.appendInt(maxYs.get(group));
                minYsBuilder.appendInt(minYs.get(group));
            }
            blocks[offset + 0] = minNegXsBuilder.build();
            blocks[offset + 1] = minPosXsBuilder.build();
            blocks[offset + 2] = maxNegXsBuilder.build();
            blocks[offset + 3] = maxPosXsBuilder.build();
            blocks[offset + 4] = maxYsBuilder.build();
            blocks[offset + 5] = minYsBuilder.build();
        }
    }

    public void add(int groupId, Geometry geo) {
        ensureCapacity(groupId);
        geoPointVisitor.reset();
        if (geo.visit(new SpatialEnvelopeVisitor(geoPointVisitor))) {
            add(
                groupId,
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getMinNegX()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getMinPosX()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getMaxNegX()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getMaxPosX()),
                POINT_TYPE.encoder().encodeY(geoPointVisitor.getMaxY()),
                POINT_TYPE.encoder().encodeY(geoPointVisitor.getMinY())
            );
        }
    }

    public void add(int groupId, SpatialExtentGroupingStateWrappedLongitudeState inState, int inPosition) {
        ensureCapacity(groupId);
        if (inState.hasValue(inPosition)) {
            add(
                groupId,
                inState.minNegXs.get(inPosition),
                inState.minPosXs.get(inPosition),
                inState.maxNegXs.get(inPosition),
                inState.maxPosXs.get(inPosition),
                inState.maxYs.get(inPosition),
                inState.minYs.get(inPosition)
            );
        }
    }

    /**
     * This method is used when the field is a geo_point or cartesian_point and is loaded from doc-values.
     * This optimization is enabled when the field has doc-values and is only used in a spatial aggregation.
     */
    public void add(int groupId, long encoded) {
        int x = POINT_TYPE.extractX(encoded);
        int y = POINT_TYPE.extractY(encoded);
        add(groupId, x, x, x, x, y, y);
    }

    /**
     * This method is used when extents are extracted from the doc-values field by the {@link GeometryDocValueReader}.
     * This optimization is enabled when the field has doc-values and is only used in the ST_EXTENT aggregation.
     */
    public void add(int groupId, int[] values) {
        if (values.length == 6) {
            // Values are stored according to the order defined in the Extent class
            int top = values[0];
            int bottom = values[1];
            int negLeft = values[2];
            int negRight = values[3];
            int posLeft = values[4];
            int posRight = values[5];
            add(groupId, negLeft, posLeft, negRight, posRight, top, bottom);
        } else {
            throw new IllegalArgumentException("Expected 6 values, got " + values.length);
        }
    }

    public void add(int groupId, int minNegX, int minPosX, int maxNegX, int maxPosX, int maxY, int minY) {
        ensureCapacity(groupId);
        if (hasValue(groupId)) {
            minNegXs.set(groupId, Math.min(minNegXs.get(groupId), minNegX));
            minPosXs.set(groupId, SpatialAggregationUtils.minPos(minPosXs.get(groupId), minPosX));
            maxNegXs.set(groupId, SpatialAggregationUtils.maxNeg(maxNegXs.get(groupId), maxNegX));
            maxPosXs.set(groupId, Math.max(maxPosXs.get(groupId), maxPosX));
            maxYs.set(groupId, Math.max(maxYs.get(groupId), maxY));
            minYs.set(groupId, Math.min(minYs.get(groupId), minY));
        } else {
            minNegXs.set(groupId, minNegX);
            minPosXs.set(groupId, minPosX);
            maxNegXs.set(groupId, maxNegX);
            maxPosXs.set(groupId, maxPosX);
            maxYs.set(groupId, maxY);
            minYs.set(groupId, minY);
        }
        trackGroupId(groupId);
    }

    private void ensureCapacity(int groupId) {
        long requiredSize = groupId + 1;
        if (minNegXs.size() < requiredSize) {
            minNegXs = bigArrays.grow(minNegXs, requiredSize);
            minPosXs = bigArrays.grow(minPosXs, requiredSize);
            maxNegXs = bigArrays.grow(maxNegXs, requiredSize);
            maxPosXs = bigArrays.grow(maxPosXs, requiredSize);
            minYs = bigArrays.grow(minYs, requiredSize);
            maxYs = bigArrays.grow(maxYs, requiredSize);
        }
    }

    public Block toBlock(IntVector selected, DriverContext driverContext) {
        try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int si = selected.getInt(i);
                if (hasValue(si)) {
                    builder.appendBytesRef(
                        new BytesRef(
                            WellKnownBinary.toWKB(
                                asRectangle(
                                    minNegXs.get(si),
                                    minPosXs.get(si),
                                    maxNegXs.get(si),
                                    maxPosXs.get(si),
                                    maxYs.get(si),
                                    minYs.get(si)
                                ),
                                ByteOrder.LITTLE_ENDIAN
                            )
                        )
                    );
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }
}
