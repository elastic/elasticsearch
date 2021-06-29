/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.literal.geo;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantNamedWriteable;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Objects;

/**
 * Wrapper class to represent a GeoShape in SQL
 *
 * It is required to override the XContent serialization. The ShapeBuilder serializes using GeoJSON by default,
 * but in SQL we need the serialization to be WKT-based.
 */
public class GeoShape implements ToXContentFragment, ConstantNamedWriteable {

    public static final String NAME = "geo";

    private final Geometry shape;

    private static final GeometryParser GEOMETRY_PARSER = new GeometryParser(true, true, true);

    public GeoShape(double lon, double lat) {
        shape = new Point(lon, lat);
    }

    public GeoShape(Object value) throws IOException {
        try {
            shape = parse(value);
        } catch (ParseException ex) {
            throw new QlIllegalArgumentException("Cannot parse [" + value + "] as a geo_shape or shape value", ex);
        }
    }

    public GeoShape(StreamInput in) throws IOException {
        String value = in.readString();
        try {
            shape = parse(value);
        } catch (ParseException ex) {
            throw new QlIllegalArgumentException("Cannot parse [" + value + "] as a geo_shape or shape value", ex);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(WellKnownText.toWKT(shape));
    }

    @Override
    public String toString() {
        return WellKnownText.toWKT(shape);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(WellKnownText.toWKT(shape));
    }

    public Geometry toGeometry() {
        return shape;
    }

    public Point firstPoint() {
        return shape.visit(new GeometryVisitor<Point, RuntimeException>() {
            @Override
            public Point visit(Circle circle) {
                return new Point(circle.getX(), circle.getY(), circle.hasZ() ? circle.getZ() : Double.NaN);
            }

            @Override
            public Point visit(GeometryCollection<?> collection) {
                if (collection.size() > 0) {
                    return collection.get(0).visit(this);
                }
                return null;
            }

            @Override
            public Point visit(Line line) {
                if (line.length() > 0) {
                    return new Point(line.getX(0), line.getY(0), line.hasZ() ? line.getZ(0) :  Double.NaN);
                }
                return null;
            }

            @Override
            public Point visit(LinearRing ring) {
                return visit((Line) ring);
            }

            @Override
            public Point visit(MultiLine multiLine) {
                return visit((GeometryCollection<?>) multiLine);
            }

            @Override
            public Point visit(MultiPoint multiPoint) {
                return visit((GeometryCollection<?>) multiPoint);
            }

            @Override
            public Point visit(MultiPolygon multiPolygon) {
                return visit((GeometryCollection<?>) multiPolygon);
            }

            @Override
            public Point visit(Point point) {
                return point;
            }

            @Override
            public Point visit(Polygon polygon) {
                return visit(polygon.getPolygon());
            }

            @Override
            public Point visit(Rectangle rectangle) {
                return new Point(rectangle.getMinX(), rectangle.getMinY(), rectangle.getMinZ());
            }
        });
    }

    public Double getX() {
        Point firstPoint = firstPoint();
        return firstPoint != null ? firstPoint.getX() : null;
    }

    public Double getY() {
        Point firstPoint = firstPoint();
        return firstPoint != null ? firstPoint.getY() : null;
    }

    public Double getZ() {
        Point firstPoint = firstPoint();
        return firstPoint != null && firstPoint.hasZ() ? firstPoint.getZ() : null;
    }

    public String getGeometryType() {
        return toGeometry().type().name();
    }

    public static double distance(GeoShape shape1, GeoShape shape2) {
        if (shape1.shape instanceof Point == false) {
            throw new QlIllegalArgumentException("distance calculation is only supported for points; received [{}]", shape1);
        }
        if (shape2.shape instanceof Point == false) {
            throw new QlIllegalArgumentException("distance calculation is only supported for points; received [{}]", shape2);
        }
        double srcLat = ((Point) shape1.shape).getY();
        double srcLon = ((Point) shape1.shape).getX();
        double dstLat = ((Point) shape2.shape).getY();
        double dstLon = ((Point) shape2.shape).getX();
        return GeoUtils.arcDistance(srcLat, srcLon, dstLat, dstLon);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeoShape geoShape = (GeoShape) o;
        return shape.equals(geoShape.shape);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shape);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static Geometry parse(Object value) throws IOException, ParseException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("value", value);
        content.endObject();

        try (InputStream stream = BytesReference.bytes(content).streamInput();
             XContentParser parser = JsonXContent.jsonXContent.createParser(
                 NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            return GEOMETRY_PARSER.parse(parser);
        }
    }
}
