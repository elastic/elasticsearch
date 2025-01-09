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
    private int maxY = Integer.MIN_VALUE;
    private int minY = Integer.MAX_VALUE;
    private int minNegX = Integer.MAX_VALUE;
    private int maxNegX = Integer.MIN_VALUE;
    private int minPosX = Integer.MAX_VALUE;
    private int maxPosX = Integer.MIN_VALUE;

    private final SpatialEnvelopeVisitor.GeoPointVisitor geoPointVisitor = new SpatialEnvelopeVisitor.GeoPointVisitor(
        SpatialEnvelopeVisitor.WrapLongitude.WRAP
    );

    @Override
    public void close() {}

    @Override
    public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        assert blocks.length >= offset + 6;
        var blockFactory = driverContext.blockFactory();
        blocks[offset + 0] = blockFactory.newConstantIntBlockWith(minNegX, 1);
        blocks[offset + 1] = blockFactory.newConstantIntBlockWith(minPosX, 1);
        blocks[offset + 2] = blockFactory.newConstantIntBlockWith(maxNegX, 1);
        blocks[offset + 3] = blockFactory.newConstantIntBlockWith(maxPosX, 1);
        blocks[offset + 4] = blockFactory.newConstantIntBlockWith(maxY, 1);
        blocks[offset + 5] = blockFactory.newConstantIntBlockWith(minY, 1);
    }

    public void add(Geometry geo) {
        geoPointVisitor.reset();
        if (geo.visit(new SpatialEnvelopeVisitor(geoPointVisitor))) {
            add(
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getMinNegX()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getMinPosX()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getMaxNegX()),
                SpatialAggregationUtils.encodeLongitude(geoPointVisitor.getMaxPosX()),
                POINT_TYPE.encoder().encodeY(geoPointVisitor.getMaxY()),
                POINT_TYPE.encoder().encodeY(geoPointVisitor.getMinY())
            );
        }
    }

    /**
     * This method is used when extents are extracted from the doc-values field by the {@link GeometryDocValueReader}.
     * This optimization is enabled when the field has doc-values and is only used in the ST_EXTENT aggregation.
     */
    public void add(int[] values) {
        if (values.length == 6) {
            // Values are stored according to the order defined in the Extent class
            int top = values[0];
            int bottom = values[1];
            int negLeft = values[2];
            int negRight = values[3];
            int posLeft = values[4];
            int posRight = values[5];
            add(negLeft, posLeft, negRight, posRight, top, bottom);
        } else {
            throw new IllegalArgumentException("Expected 6 values, got " + values.length);
        }
    }

    public void add(int minNegX, int minPosX, int maxNegX, int maxPosX, int maxY, int minY) {
        seen = true;
        this.minNegX = Math.min(this.minNegX, minNegX);
        this.minPosX = SpatialAggregationUtils.minPos(this.minPosX, minPosX);
        this.maxNegX = SpatialAggregationUtils.maxNeg(this.maxNegX, maxNegX);
        this.maxPosX = Math.max(this.maxPosX, maxPosX);
        this.maxY = Math.max(this.maxY, maxY);
        this.minY = Math.min(this.minY, minY);
    }

    /**
     * This method is used when the field is a geo_point or cartesian_point and is loaded from doc-values.
     * This optimization is enabled when the field has doc-values and is only used in a spatial aggregation.
     */
    public void add(long encoded) {
        int x = POINT_TYPE.extractX(encoded);
        int y = POINT_TYPE.extractY(encoded);
        add(x, x, x, x, y, y);
    }

    public Block toBlock(DriverContext driverContext) {
        var factory = driverContext.blockFactory();
        return seen ? factory.newConstantBytesRefBlockWith(new BytesRef(toWKB()), 1) : factory.newConstantNullBlock(1);
    }

    private byte[] toWKB() {
        return WellKnownBinary.toWKB(asRectangle(minNegX, minPosX, maxNegX, maxPosX, maxY, minY), ByteOrder.LITTLE_ENDIAN);
    }

    static Rectangle asRectangle(int minNegX, int minPosX, int maxNegX, int maxPosX, int maxY, int minY) {
        return SpatialEnvelopeVisitor.GeoPointVisitor.getResult(
            minNegX <= 0 ? decodeLongitude(minNegX) : Double.POSITIVE_INFINITY,
            minPosX >= 0 ? decodeLongitude(minPosX) : Double.POSITIVE_INFINITY,
            maxNegX <= 0 ? decodeLongitude(maxNegX) : Double.NEGATIVE_INFINITY,
            maxPosX >= 0 ? decodeLongitude(maxPosX) : Double.NEGATIVE_INFINITY,
            GeoEncodingUtils.decodeLatitude(maxY),
            GeoEncodingUtils.decodeLatitude(minY),
            SpatialEnvelopeVisitor.WrapLongitude.WRAP
        );
    }
}
