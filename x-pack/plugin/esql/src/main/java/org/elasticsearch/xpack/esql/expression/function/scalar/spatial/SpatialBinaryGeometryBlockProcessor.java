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
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.operation.union.UnaryUnionOp;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Shared block-processing logic for spatial functions that combine two geometry arguments
 * (e.g. ST_UNION, ST_INTERSECTION, ST_DIFFERENCE, ST_SYMDIFFERENCE).
 */
class SpatialBinaryGeometryBlockProcessor {
    private final SpatialCoordinateTypes coordinateType;
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final BiFunction<Geometry, Geometry, Geometry> operation;

    SpatialBinaryGeometryBlockProcessor(SpatialCoordinateTypes coordinateType, BiFunction<Geometry, Geometry, Geometry> operation) {
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
            Geometry leftJts = fromBytesRefBlock(left, p);
            Geometry rightJts = fromBytesRefBlock(right, p);
            builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(operation.apply(leftJts, rightJts)));
        } catch (ParseException e) {
            throw new IllegalArgumentException("could not parse the geometry expression: " + e.getMessage(), e);
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
            Geometry leftJts = fromLongBlock(left, p);
            Geometry rightJts = fromBytesRefBlock(right, p);
            builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(operation.apply(leftJts, rightJts)));
        } catch (ParseException e) {
            throw new IllegalArgumentException("could not parse the geometry expression: " + e.getMessage(), e);
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
            Geometry leftJts = fromBytesRefBlock(left, p);
            Geometry rightJts = fromLongBlock(right, p);
            builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(operation.apply(leftJts, rightJts)));
        } catch (ParseException e) {
            throw new IllegalArgumentException("could not parse the geometry expression: " + e.getMessage(), e);
        }
    }

    /**
     * Process two doc-values (long-encoded) point blocks at position {@code p}.
     */
    void processBothDocValues(BytesRefBlock.Builder builder, int p, LongBlock left, LongBlock right) {
        if (left.getValueCount(p) < 1 || right.getValueCount(p) < 1) {
            builder.appendNull();
            return;
        }
        Geometry leftJts = fromLongBlock(left, p);
        Geometry rightJts = fromLongBlock(right, p);
        builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(operation.apply(leftJts, rightJts)));
    }

    private Geometry fromBytesRefBlock(BytesRefBlock block, int p) throws ParseException {
        int firstValueIndex = block.getFirstValueIndex(p);
        int valueCount = block.getValueCount(p);
        BytesRef scratch = new BytesRef();
        if (valueCount == 1) {
            return flattenIfHeterogeneousCollection(UNSPECIFIED.wkbToJtsGeometry(block.getBytesRef(firstValueIndex, scratch)));
        }
        List<Geometry> geometries = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            geometries.add(UNSPECIFIED.wkbToJtsGeometry(block.getBytesRef(firstValueIndex + i, scratch)));
        }
        // Use UnaryUnionOp to combine multiple block values into one geometry for the operation.
        return UnaryUnionOp.union(geometries);
    }

    /**
     * JTS binary overlay operations (union, intersection, difference, symdifference) reject
     * heterogeneous {@link GeometryCollection} arguments with an IllegalArgumentException.
     * Homogeneous subtypes (MultiPoint, MultiLineString, MultiPolygon) are supported.
     * For anything else (a true heterogeneous collection), pre-flatten to a supported type
     * using a self-union so the binary operation can proceed.
     */
    static Geometry flattenIfHeterogeneousCollection(Geometry geom) {
        if (geom instanceof MultiPoint || geom instanceof MultiLineString || geom instanceof MultiPolygon) {
            return geom;
        }
        if (geom instanceof GeometryCollection) {
            return UnaryUnionOp.union(geom);
        }
        return geom;
    }

    private Geometry fromLongBlock(LongBlock block, int p) {
        int firstValueIndex = block.getFirstValueIndex(p);
        int valueCount = block.getValueCount(p);
        if (valueCount == 1) {
            Point point = coordinateType.longAsPoint(block.getLong(firstValueIndex));
            return geometryFactory.createPoint(new Coordinate(point.getX(), point.getY()));
        }
        Coordinate[] coords = new Coordinate[valueCount];
        for (int i = 0; i < valueCount; i++) {
            Point point = coordinateType.longAsPoint(block.getLong(firstValueIndex + i));
            coords[i] = new Coordinate(point.getX(), point.getY());
        }
        return geometryFactory.createMultiPointFromCoords(coords);
    }
}
