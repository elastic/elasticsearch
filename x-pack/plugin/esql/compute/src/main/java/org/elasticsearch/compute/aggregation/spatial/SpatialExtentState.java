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
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;

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
            add(Math.min(negLeft, posLeft), Math.max(negRight, posRight), top, bottom);
        } else if (values.length == 4) {
            // Values are stored according to the order defined in the Rectangle class
            int minX = values[0];
            int maxX = values[1];
            int maxY = values[2];
            int minY = values[3];
            add(minX, maxX, maxY, minY);
        } else {
            throw new IllegalArgumentException("Expected 4 or 6 values, got " + values.length);
        }
    }

    public void add(int minX, int maxX, int maxY, int minY) {
        seen = true;
        this.minX = Math.min(this.minX, minX);
        this.maxX = Math.max(this.maxX, maxX);
        this.maxY = Math.max(this.maxY, maxY);
        this.minY = Math.min(this.minY, minY);
    }

    /**
     * This method is used when the field is a geo_point or cartesian_point and is loaded from doc-values.
     * This optimization is enabled when the field has doc-values and is only used in a spatial aggregation.
     */
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
