/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;

/**
 * Wrapper class to represent a GeoShape in SQL
 *
 * It is required to override the XContent serialization. The ShapeBuilder serializes using GeoJSON by default,
 * but in SQL we need the serialization to be WKT-based.
 */
public class GeoShape implements ToXContentFragment {

    private final ShapeBuilder<?, ?> shapeBuilder;

    public GeoShape(GeoPoint point) {
        shapeBuilder = new PointBuilder(point.getLon(), point.getLat());
    }

    public GeoShape(Object value) throws IOException {
        shapeBuilder = ShapeParser.parse(value);
    }

    @Override
    public String toString() {
        return shapeBuilder.toWKT();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(shapeBuilder.toWKT());
    }

    public int dimension() {
        return shapeBuilder.inherentDimensions();
    }

    public String geometryType() {
        return geometryType(shapeBuilder.type());
    }

    static String geometryType(GeoShapeType type) {
        switch (type) {
            case POINT: return "Point";
            case CIRCLE: return "Circle";
            case POLYGON: return "Polygon";
            case ENVELOPE: return "Envelope";
            case LINESTRING: return "LineString";
            case MULTIPOINT: return "MultiPoint";
            case MULTIPOLYGON: return "MultiPolygon";
            case MULTILINESTRING: return "MultiLineString";
            case GEOMETRYCOLLECTION: return "GeometryCollection";
            default:
                throw new SqlIllegalArgumentException("Unsupported geometry type [" + type + "]");

        }
    }

    public double x() {
        return shapeBuilder.firstX();
    }

    public double y() {
        return shapeBuilder.firstY();
    }

    public double minX() {
        return shapeBuilder.buildS4J().getBoundingBox().getMinX();
    }

    public double minY() {
        return shapeBuilder.buildS4J().getBoundingBox().getMinY();
    }

    public double maxX() {
        return shapeBuilder.buildS4J().getBoundingBox().getMaxX();
    }

    public double maxY() {
        return shapeBuilder.buildS4J().getBoundingBox().getMaxY();
    }
}
