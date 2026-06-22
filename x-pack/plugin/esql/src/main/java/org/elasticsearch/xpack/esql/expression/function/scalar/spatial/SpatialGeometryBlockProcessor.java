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
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Shared block-processing logic for spatial functions that transform a geometry with a double parameter
 * (e.g. ST_SIMPLIFY, ST_SIMPLIFYPRESERVETOPOLOGY, ST_BUFFER).
 */
class SpatialGeometryBlockProcessor {
    private final SpatialCoordinateTypes spatialCoordinateType;
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final BiFunction<Geometry, Double, Geometry> operation;

    SpatialGeometryBlockProcessor(SpatialCoordinateTypes spatialCoordinateType, BiFunction<Geometry, Double, Geometry> operation) {
        this.spatialCoordinateType = spatialCoordinateType;
        this.operation = operation;
    }

    BytesRef processSingleGeometry(BytesRef inputGeometry, double parameter) {
        if (inputGeometry == null) {
            return null;
        }
        try {
            return processSingleGeometry(UNSPECIFIED.wkbToJtsGeometry(inputGeometry), parameter);
        } catch (ParseException e) {
            throw new IllegalArgumentException("could not parse the geometry expression: " + e);
        }
    }

    BytesRef processSingleGeometry(Geometry jtsGeometry, double parameter) {
        Geometry result = applyOperation(jtsGeometry, parameter);
        return UNSPECIFIED.jtsGeometryToWkb(result);
    }

    void processPoints(BytesRefBlock.Builder builder, int p, LongBlock left, double parameter) throws IOException {
        if (left.getValueCount(p) < 1) {
            builder.appendNull();
        } else {
            final Geometry jtsGeometry = asJtsMultiPoint(left, p, spatialCoordinateType::longAsPoint);
            Geometry result = applyOperation(jtsGeometry, parameter);
            builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(result));
        }
    }

    void processGeometries(BytesRefBlock.Builder builder, int p, BytesRefBlock left, double parameter) {
        if (left.getValueCount(p) < 1) {
            builder.appendNull();
        } else {
            final Geometry jtsGeometry = asJtsGeometry(left, p);
            Geometry result = applyOperation(jtsGeometry, parameter);
            builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(result));
        }
    }

    private Geometry applyOperation(Geometry jtsGeometry, double parameter) {
        try {
            return operation.apply(jtsGeometry, parameter);
        } catch (StackOverflowError e) {
            // ST_SIMPLIFY (DouglasPeuckerSimplifier) uses IterativeDouglasPeuckerSimplifier,
            // which is heap-allocated and cannot cause StackOverflowError. This catch exists
            // for ST_SIMPLIFYPRESERVETOPOLOGY, whose JTS implementation (TaggedLineStringSimplifier)
            // uses call-stack recursion proportional to vertex count. Porting that algorithm to an
            // iterative form would require reimplementing ~500 lines of topology-aware JTS internals,
            // so catching StackOverflowError here is the pragmatic safety net for that function only.
            throw new IllegalArgumentException(
                "geometry processing failed due to excessive recursion depth; the geometry has "
                    + jtsGeometry.getNumPoints()
                    + " vertices which may be too complex for the given parameter value. "
                    + "Consider reducing the number of vertices or using a larger tolerance."
            );
        }
    }

    Geometry asJtsMultiPoint(LongBlock valueBlock, int position, Function<Long, Point> decoder) {
        final int firstValueIndex = valueBlock.getFirstValueIndex(position);
        final int valueCount = valueBlock.getValueCount(position);
        if (valueCount == 1) {
            Point point = decoder.apply(valueBlock.getLong(firstValueIndex));
            return geometryFactory.createPoint(new Coordinate(point.getX(), point.getY()));
        }
        final Coordinate[] coordinates = new Coordinate[valueCount];
        for (int i = 0; i < valueCount; i++) {
            Point point = decoder.apply(valueBlock.getLong(firstValueIndex + i));
            coordinates[i] = new Coordinate(point.getX(), point.getY());
        }
        return geometryFactory.createMultiPointFromCoords(coordinates);
    }

    Geometry asJtsGeometry(BytesRefBlock valueBlock, int position) {
        try {
            final int firstValueIndex = valueBlock.getFirstValueIndex(position);
            final int valueCount = valueBlock.getValueCount(position);
            BytesRef scratch = new BytesRef();
            if (valueCount == 1) {
                return UNSPECIFIED.wkbToJtsGeometry(valueBlock.getBytesRef(firstValueIndex, scratch));
            }
            final Geometry[] geometries = new Geometry[valueCount];
            for (int i = 0; i < valueCount; i++) {
                geometries[i] = UNSPECIFIED.wkbToJtsGeometry(valueBlock.getBytesRef(firstValueIndex, scratch));
            }
            return geometryFactory.createGeometryCollection(geometries);
        } catch (ParseException e) {
            throw new IllegalArgumentException("could not parse the geometry expression: " + e);
        }
    }

    Geometry asJtsGeometry(List<?> values) {
        try {
            final Geometry[] geometries = new Geometry[values.size()];
            for (int i = 0; i < values.size(); i++) {
                if (values.get(i) instanceof BytesRef inputGeometry) {
                    geometries[i] = UNSPECIFIED.wkbToJtsGeometry(inputGeometry);
                } else {
                    throw new IllegalArgumentException("unsupported list element type: " + values.get(i).getClass().getSimpleName());
                }
            }
            return geometryFactory.createGeometryCollection(geometries);
        } catch (ParseException e) {
            throw new IllegalArgumentException("could not parse the geometry expression: " + e);
        }
    }
}
