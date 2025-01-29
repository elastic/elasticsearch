/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;

import java.nio.ByteOrder;

import static org.elasticsearch.compute.aggregation.spatial.SpatialAggregationUtils.decodeLongitude;

final class SpatialExtentStateWrappedLongitudeState implements AggregatorState {
    // Only geo points support longitude wrapping.
    private static final PointType POINT_TYPE = PointType.GEO;
    private boolean seen = false;
    private int top = Integer.MIN_VALUE;
    private int bottom = Integer.MAX_VALUE;
    private int negLeft = Integer.MAX_VALUE;
    private int negRight = Integer.MIN_VALUE;
    private int posLeft = Integer.MAX_VALUE;
    private int posRight = Integer.MIN_VALUE;

    private final SpatialEnvelopeVisitor.GeoPointVisitor geoPointVisitor = new SpatialEnvelopeVisitor.GeoPointVisitor(
        SpatialEnvelopeVisitor.WrapLongitude.WRAP
    );

    @Override
    public void close() {}

    @Override
    public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        assert blocks.length >= offset + 6;
        var blockFactory = driverContext.blockFactory();
        blocks[offset + 0] = blockFactory.newConstantIntBlockWith(top, 1);
        blocks[offset + 1] = blockFactory.newConstantIntBlockWith(bottom, 1);
        blocks[offset + 2] = blockFactory.newConstantIntBlockWith(negLeft, 1);
        blocks[offset + 3] = blockFactory.newConstantIntBlockWith(negRight, 1);
        blocks[offset + 4] = blockFactory.newConstantIntBlockWith(posLeft, 1);
        blocks[offset + 5] = blockFactory.newConstantIntBlockWith(posRight, 1);
    }

    public void add(Geometry geo) {
        geoPointVisitor.reset();
        if (geo.visit(new SpatialEnvelopeVisitor(geoPointVisitor))) {
            add(
                POINT_TYPE.encoder().encodeY(geoPointVisitor.getTop()),
                POINT_TYPE.encoder().encodeY(geoPointVisitor.getBottom()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getNegLeft()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getNegRight()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getPosLeft()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getPosRight())
            );
        }
    }

    /**
     * This method is used when extents are extracted from the doc-values field by the {@link GeometryDocValueReader}.
     * This optimization is enabled when the field has doc-values and is only used in the ST_EXTENT aggregation.
     */
    public void add(int[] values) {
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
        add(top, bottom, negLeft, negRight, posLeft, posRight);
    }

    public void add(int top, int bottom, int negLeft, int negRight, int posLeft, int posRight) {
        seen = true;
        this.top = Math.max(this.top, top);
        this.bottom = Math.min(this.bottom, bottom);
        this.negLeft = Math.min(this.negLeft, negLeft);
        this.negRight = SpatialAggregationUtils.maxNeg(this.negRight, negRight);
        this.posLeft = SpatialAggregationUtils.minPos(this.posLeft, posLeft);
        this.posRight = Math.max(this.posRight, posRight);
    }

    /**
     * This method is used when the field is a geo_point or cartesian_point and is loaded from doc-values.
     * This optimization is enabled when the field has doc-values and is only used in a spatial aggregation.
     */
    public void add(long encoded) {
        int x = POINT_TYPE.extractX(encoded);
        int y = POINT_TYPE.extractY(encoded);
        add(y, y, x, x, x, x);
    }

    public Block toBlock(DriverContext driverContext) {
        var factory = driverContext.blockFactory();
        return seen ? factory.newConstantBytesRefBlockWith(new BytesRef(toWKB()), 1) : factory.newConstantNullBlock(1);
    }

    private byte[] toWKB() {
        return WellKnownBinary.toWKB(asRectangle(top, bottom, negLeft, negRight, posLeft, posRight), ByteOrder.LITTLE_ENDIAN);
    }

    static Rectangle asRectangle(int top, int bottom, int negLeft, int negRight, int posLeft, int posRight) {
        return SpatialEnvelopeVisitor.GeoPointVisitor.getResult(
            GeoEncodingUtils.decodeLatitude(top),
            GeoEncodingUtils.decodeLatitude(bottom),
            negLeft <= 0 ? decodeLongitude(negLeft) : Double.POSITIVE_INFINITY,
            negRight <= 0 ? decodeLongitude(negRight) : Double.NEGATIVE_INFINITY,
            posLeft >= 0 ? decodeLongitude(posLeft) : Double.POSITIVE_INFINITY,
            posRight >= 0 ? decodeLongitude(posRight) : Double.NEGATIVE_INFINITY,
            SpatialEnvelopeVisitor.WrapLongitude.WRAP
        );
    }
}
