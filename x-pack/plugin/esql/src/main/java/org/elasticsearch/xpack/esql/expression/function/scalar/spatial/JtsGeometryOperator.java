/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.operation.union.UnaryUnionOp;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Pre-built {@link GeometryOperator} constants that decode WKB to JTS, delegate directly to JTS
 * geometry operations, and re-encode the result as WKB. These are thin wrappers around the
 * corresponding {@code Geometry} instance methods.
 */
public final class JtsGeometryOperator {

    /** Computes the geometric intersection of two geometries. */
    public static final GeometryOperator INTERSECTION = (wkbA, wkbB) -> {
        try {
            Geometry a = flatten(UNSPECIFIED.wkbToJtsGeometry(wkbA));
            Geometry b = flatten(UNSPECIFIED.wkbToJtsGeometry(wkbB));
            return UNSPECIFIED.jtsGeometryToWkb(a.intersection(b));
        } catch (ParseException e) {
            throw new IOException("Failed to decode WKB for JTS operation", e);
        }
    };

    /** Computes the geometric union of two geometries. */
    public static final GeometryOperator UNION = (wkbA, wkbB) -> {
        try {
            Geometry a = flatten(UNSPECIFIED.wkbToJtsGeometry(wkbA));
            Geometry b = flatten(UNSPECIFIED.wkbToJtsGeometry(wkbB));
            return UNSPECIFIED.jtsGeometryToWkb(a.union(b));
        } catch (ParseException e) {
            throw new IOException("Failed to decode WKB for JTS operation", e);
        }
    };

    /** Computes the part of {@code a} that does not intersect with {@code b}. */
    public static final GeometryOperator DIFFERENCE = (wkbA, wkbB) -> {
        try {
            Geometry a = flatten(UNSPECIFIED.wkbToJtsGeometry(wkbA));
            Geometry b = flatten(UNSPECIFIED.wkbToJtsGeometry(wkbB));
            return UNSPECIFIED.jtsGeometryToWkb(a.difference(b));
        } catch (ParseException e) {
            throw new IOException("Failed to decode WKB for JTS operation", e);
        }
    };

    /** Computes the symmetric difference of two geometries. */
    public static final GeometryOperator SYM_DIFFERENCE = (wkbA, wkbB) -> {
        try {
            Geometry a = flatten(UNSPECIFIED.wkbToJtsGeometry(wkbA));
            Geometry b = flatten(UNSPECIFIED.wkbToJtsGeometry(wkbB));
            return UNSPECIFIED.jtsGeometryToWkb(a.symDifference(b));
        } catch (ParseException e) {
            throw new IOException("Failed to decode WKB for JTS operation", e);
        }
    };

    /**
     * JTS binary overlay operations reject heterogeneous {@link GeometryCollection} arguments.
     * Homogeneous subtypes (MultiPoint, MultiLineString, MultiPolygon) are accepted as-is.
     * A heterogeneous collection is pre-flattened to a supported homogeneous type via a self-union.
     * This handles both multi-value geo_shape fields (stored as a single GEOMETRYCOLLECTION WKB)
     * and multiple WKB values combined into a collection by the block processor.
     */
    static Geometry flatten(Geometry geom) {
        if (geom instanceof MultiPoint || geom instanceof MultiLineString || geom instanceof MultiPolygon) {
            return geom;
        }
        if (geom instanceof GeometryCollection) {
            return UnaryUnionOp.union(geom);
        }
        return geom;
    }

    private JtsGeometryOperator() {}
}
