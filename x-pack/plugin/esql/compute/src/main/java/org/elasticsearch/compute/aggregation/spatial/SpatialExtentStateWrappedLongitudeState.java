/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.WellKnownBinary;

import java.nio.ByteOrder;

final class SpatialExtentStateWrappedLongitudeState implements AggregatorState {
    // Only geo points support longitude wrapping.
    private static final PointType POINT_TYPE = PointType.GEO;
    private boolean seen = false;
    private int minNegX = SpatialAggregationUtils.DEFAULT_NEG;
    private int minPosX = SpatialAggregationUtils.DEFAULT_POS;
    private int maxNegX = SpatialAggregationUtils.DEFAULT_NEG;
    private int maxPosX = SpatialAggregationUtils.DEFAULT_POS;
    private int maxY = Integer.MIN_VALUE;
    private int minY = Integer.MAX_VALUE;

    private GeoPointEnvelopeVisitor geoPointVisitor = new GeoPointEnvelopeVisitor();

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
                SpatialAggregationUtils.encodeNegativeLongitude(geoPointVisitor.getMinNegX()),
                SpatialAggregationUtils.encodePositiveLongitude(geoPointVisitor.getMinPosX()),
                SpatialAggregationUtils.encodeNegativeLongitude(geoPointVisitor.getMaxNegX()),
                SpatialAggregationUtils.encodePositiveLongitude(geoPointVisitor.getMaxPosX()),
                POINT_TYPE.encoder().encodeY(geoPointVisitor.getMaxY()),
                POINT_TYPE.encoder().encodeY(geoPointVisitor.getMinY())
            );
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
        assert this.minNegX <= 0 == this.maxNegX <= 0 : "minNegX=" + this.minNegX + " maxNegX=" + this.maxNegX;
        assert this.minPosX >= 0 == this.maxPosX >= 0 : "minPosX=" + this.minPosX + " maxPosX=" + this.maxPosX;
    }

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
        return WellKnownBinary.toWKB(
            SpatialAggregationUtils.asRectangle(minNegX, minPosX, maxNegX, maxPosX, maxY, minY),
            ByteOrder.LITTLE_ENDIAN
        );
    }
}
