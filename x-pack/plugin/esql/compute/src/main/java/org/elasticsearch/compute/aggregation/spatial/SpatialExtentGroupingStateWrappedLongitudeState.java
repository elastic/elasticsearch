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
    private IntArray tops;
    private IntArray bottoms;
    private IntArray negLefts;
    private IntArray negRights;
    private IntArray posLefts;
    private IntArray posRights;

    private final SpatialEnvelopeVisitor.GeoPointVisitor geoPointVisitor;

    SpatialExtentGroupingStateWrappedLongitudeState() {
        this(BigArrays.NON_RECYCLING_INSTANCE);
    }

    SpatialExtentGroupingStateWrappedLongitudeState(BigArrays bigArrays) {
        super(bigArrays);
        this.tops = bigArrays.newIntArray(0, false);
        this.bottoms = bigArrays.newIntArray(0, false);
        this.negLefts = bigArrays.newIntArray(0, false);
        this.negRights = bigArrays.newIntArray(0, false);
        this.posLefts = bigArrays.newIntArray(0, false);
        this.posRights = bigArrays.newIntArray(0, false);
        enableGroupIdTracking(new SeenGroupIds.Empty());
        this.geoPointVisitor = new SpatialEnvelopeVisitor.GeoPointVisitor(SpatialEnvelopeVisitor.WrapLongitude.WRAP);
    }

    @Override
    public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        assert blocks.length >= offset;
        try (
            var topsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var bottomsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var negLeftsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var negRightsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var posLeftsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
            var posRightsBuilder = driverContext.blockFactory().newIntBlockBuilder(selected.getPositionCount());
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    topsBuilder.appendInt(tops.get(group));
                    bottomsBuilder.appendInt(bottoms.get(group));
                    negLeftsBuilder.appendInt(negLefts.get(group));
                    negRightsBuilder.appendInt(negRights.get(group));
                    posLeftsBuilder.appendInt(posLefts.get(group));
                    posRightsBuilder.appendInt(posRights.get(group));
                } else {
                    // TODO: Should we add Nulls here instead?
                    topsBuilder.appendInt(Integer.MIN_VALUE);
                    bottomsBuilder.appendInt(Integer.MAX_VALUE);
                    negLeftsBuilder.appendInt(Integer.MAX_VALUE);
                    negRightsBuilder.appendInt(Integer.MIN_VALUE);
                    posLeftsBuilder.appendInt(Integer.MAX_VALUE);
                    posRightsBuilder.appendInt(Integer.MIN_VALUE);
                }
            }
            blocks[offset + 0] = topsBuilder.build();
            blocks[offset + 1] = bottomsBuilder.build();
            blocks[offset + 2] = negLeftsBuilder.build();
            blocks[offset + 3] = negRightsBuilder.build();
            blocks[offset + 4] = posLeftsBuilder.build();
            blocks[offset + 5] = posRightsBuilder.build();
        }
    }

    public void add(int groupId, Geometry geo) {
        ensureCapacity(groupId);
        geoPointVisitor.reset();
        if (geo.visit(new SpatialEnvelopeVisitor(geoPointVisitor))) {
            add(
                groupId,
                POINT_TYPE.encoder().encodeY(geoPointVisitor.getTop()),
                POINT_TYPE.encoder().encodeY(geoPointVisitor.getBottom()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getNegLeft()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getNegRight()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getPosLeft()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getPosRight())
            );
        }
    }

    public void add(int groupId, SpatialExtentGroupingStateWrappedLongitudeState inState, int inPosition) {
        ensureCapacity(groupId);
        if (inState.hasValue(inPosition)) {
            add(
                groupId,
                inState.tops.get(inPosition),
                inState.bottoms.get(inPosition),
                inState.negLefts.get(inPosition),
                inState.negRights.get(inPosition),
                inState.posLefts.get(inPosition),
                inState.posRights.get(inPosition)
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
        add(groupId, y, y, x, x, x, x);
    }

    /**
     * This method is used when extents are extracted from the doc-values field by the {@link GeometryDocValueReader}.
     * This optimization is enabled when the field has doc-values and is only used in the ST_EXTENT aggregation.
     */
    public void add(int groupId, int[] values) {
        if (values.length != 6) {
            throw new IllegalArgumentException("Expected 6 values, got " + values.length);
        }
        // Values are stored according to the order defined in the Extent class
        int top = values[0];
        int bottom = values[1];
        int negLeft = values[2];
        int negRight = values[3];
        int posLeft = values[4];
        int posRight = values[5];
        add(groupId, top, bottom, negLeft, negRight, posLeft, posRight);
    }

    public void add(int groupId, int top, int bottom, int negLeft, int negRight, int posLeft, int posRight) {
        ensureCapacity(groupId);
        if (hasValue(groupId)) {
            tops.set(groupId, Math.max(tops.get(groupId), top));
            bottoms.set(groupId, Math.min(bottoms.get(groupId), bottom));
            negLefts.set(groupId, Math.min(negLefts.get(groupId), negLeft));
            negRights.set(groupId, SpatialAggregationUtils.maxNeg(negRights.get(groupId), negRight));
            posLefts.set(groupId, SpatialAggregationUtils.minPos(posLefts.get(groupId), posLeft));
            posRights.set(groupId, Math.max(posRights.get(groupId), posRight));
        } else {
            tops.set(groupId, top);
            bottoms.set(groupId, bottom);
            negLefts.set(groupId, negLeft);
            negRights.set(groupId, negRight);
            posLefts.set(groupId, posLeft);
            posRights.set(groupId, posRight);
        }
        trackGroupId(groupId);
    }

    private void ensureCapacity(int groupId) {
        long requiredSize = groupId + 1;
        if (negLefts.size() < requiredSize) {
            tops = bigArrays.grow(tops, requiredSize);
            bottoms = bigArrays.grow(bottoms, requiredSize);
            negLefts = bigArrays.grow(negLefts, requiredSize);
            negRights = bigArrays.grow(negRights, requiredSize);
            posLefts = bigArrays.grow(posLefts, requiredSize);
            posRights = bigArrays.grow(posRights, requiredSize);
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
                                    tops.get(si),
                                    bottoms.get(si),
                                    negLefts.get(si),
                                    negRights.get(si),
                                    posLefts.get(si),
                                    posRights.get(si)
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
