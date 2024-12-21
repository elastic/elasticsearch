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
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;

import java.nio.ByteOrder;

final class SpatialExtentState implements AggregatorState {
    private final PointType pointType;
    private boolean seen = false;
    private int minX = Integer.MAX_VALUE;
    private int maxX = Integer.MIN_VALUE;
    private int maxY = Integer.MIN_VALUE;
    private int minY = Integer.MAX_VALUE;

    SpatialExtentState(PointType pointType) {
        this.pointType = pointType;
    }

    @Override
    public void close() {}

    @Override
    public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        assert blocks.length >= offset + 4;
        var blockFactory = driverContext.blockFactory();
        blocks[offset + 0] = blockFactory.newConstantIntBlockWith(minX, 1);
        blocks[offset + 1] = blockFactory.newConstantIntBlockWith(maxX, 1);
        blocks[offset + 2] = blockFactory.newConstantIntBlockWith(maxY, 1);
        blocks[offset + 3] = blockFactory.newConstantIntBlockWith(minY, 1);
    }

    public void add(Geometry geo) {
        pointType.computeEnvelope(geo)
            .ifPresent(
                r -> add(
                    pointType.encoder().encodeX(r.getMinX()),
                    pointType.encoder().encodeX(r.getMaxX()),
                    pointType.encoder().encodeY(r.getMaxY()),
                    pointType.encoder().encodeY(r.getMinY())
                )
            );
    }

    public void add(int minX, int maxX, int maxY, int minY) {
        seen = true;
        this.minX = Math.min(this.minX, minX);
        this.maxX = Math.max(this.maxX, maxX);
        this.maxY = Math.max(this.maxY, maxY);
        this.minY = Math.min(this.minY, minY);
    }

    public void add(long encoded) {
        int x = pointType.extractX(encoded);
        int y = pointType.extractY(encoded);
        add(x, x, y, y);
    }

    public Block toBlock(DriverContext driverContext) {
        var factory = driverContext.blockFactory();
        return seen ? factory.newConstantBytesRefBlockWith(new BytesRef(toWKB()), 1) : factory.newConstantNullBlock(1);
    }

    private byte[] toWKB() {
        CoordinateEncoder encoder = pointType.encoder();
        return WellKnownBinary.toWKB(
            new Rectangle(encoder.decodeX(minX), encoder.decodeX(maxX), encoder.decodeY(maxY), encoder.decodeY(minY)),
            ByteOrder.LITTLE_ENDIAN
        );
    }
}
