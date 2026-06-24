/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Shared block-processing logic for spatial functions that combine two geometry arguments
 * (e.g. ST_UNION, ST_INTERSECTION, ST_DIFFERENCE, ST_SYMDIFFERENCE).
 */
class SpatialBinaryGeometryBlockProcessor {
    private final SpatialCoordinateTypes coordinateType;
    private final GeometryOperator operation;

    SpatialBinaryGeometryBlockProcessor(SpatialCoordinateTypes coordinateType, GeometryOperator operation) {
        this.coordinateType = coordinateType;
        this.operation = operation;
    }

    /**
     * Process two source (WKB) blocks at position {@code p}.
     */
    void processSourceAndSource(BytesRefBlock.Builder builder, int p, BytesRefBlock left, BytesRefBlock right) {
        if (left.getValueCount(p) < 1 || right.getValueCount(p) < 1) {
            builder.appendNull();
            return;
        }
        try {
            BytesRef leftWkb = wkbFromBytesRef(left, p);
            BytesRef rightWkb = wkbFromBytesRef(right, p);
            builder.appendBytesRef(operation.apply(leftWkb, rightWkb));
        } catch (IOException e) {
            throw new IllegalArgumentException("could not evaluate the spatial geometry operation: " + e.getMessage(), e);
        }
    }

    /**
     * Process a doc-values (long-encoded) point block on the left and a source (WKB) block on the right.
     */
    void processDocValuesAndSource(BytesRefBlock.Builder builder, int p, LongBlock left, BytesRefBlock right) {
        if (left.getValueCount(p) < 1 || right.getValueCount(p) < 1) {
            builder.appendNull();
            return;
        }
        try {
            BytesRef leftWkb = wkbFromLongBlock(left, p);
            BytesRef rightWkb = wkbFromBytesRef(right, p);
            builder.appendBytesRef(operation.apply(leftWkb, rightWkb));
        } catch (IOException e) {
            throw new IllegalArgumentException("could not evaluate the spatial geometry operation: " + e.getMessage(), e);
        }
    }

    /**
     * Process a source (WKB) block on the left and a doc-values (long-encoded) point block on the right.
     */
    void processSourceAndDocValues(BytesRefBlock.Builder builder, int p, BytesRefBlock left, LongBlock right) {
        if (left.getValueCount(p) < 1 || right.getValueCount(p) < 1) {
            builder.appendNull();
            return;
        }
        try {
            BytesRef leftWkb = wkbFromBytesRef(left, p);
            BytesRef rightWkb = wkbFromLongBlock(right, p);
            builder.appendBytesRef(operation.apply(leftWkb, rightWkb));
        } catch (IOException e) {
            throw new IllegalArgumentException("could not evaluate the spatial geometry operation: " + e.getMessage(), e);
        }
    }

    /**
     * Process two doc-values (long-encoded) point blocks at position {@code p}.
     */
    void processBothDocValues(BytesRefBlock.Builder builder, int p, LongBlock left, LongBlock right) {
        try {
            BytesRef leftWkb = wkbFromLongBlock(left, p);
            BytesRef rightWkb = wkbFromLongBlock(right, p);
            builder.appendBytesRef(operation.apply(leftWkb, rightWkb));
        } catch (IOException e) {
            throw new IllegalArgumentException("could not evaluate the spatial geometry operation: " + e.getMessage(), e);
        }
    }

    private BytesRef wkbFromBytesRef(BytesRefBlock block, int p) throws IOException {
        int firstValueIndex = block.getFirstValueIndex(p);
        int valueCount = block.getValueCount(p);
        BytesRef scratch = new BytesRef();
        if (valueCount == 1) {
            return block.getBytesRef(firstValueIndex, scratch);
        }
        // Multiple WKB values: combine into a GeometryCollection
        List<org.elasticsearch.geometry.Geometry> geoms = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            geoms.add(UNSPECIFIED.wkbToGeometry(block.getBytesRef(firstValueIndex + i, scratch)));
        }
        return UNSPECIFIED.asWkb(new GeometryCollection<>(geoms));
    }

    private BytesRef wkbFromLongBlock(LongBlock block, int p) {
        int firstValueIndex = block.getFirstValueIndex(p);
        int valueCount = block.getValueCount(p);
        if (valueCount == 1) {
            Point pt = coordinateType.longAsPoint(block.getLong(firstValueIndex));
            return coordinateType.asWkb(pt);
        }
        List<Point> pts = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            pts.add(coordinateType.longAsPoint(block.getLong(firstValueIndex + i)));
        }
        return coordinateType.asWkb(new MultiPoint(pts));
    }
}
