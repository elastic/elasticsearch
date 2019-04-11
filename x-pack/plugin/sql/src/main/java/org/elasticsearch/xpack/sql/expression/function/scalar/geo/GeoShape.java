/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.GeometryCollection;
import org.elasticsearch.geo.geometry.GeometryVisitor;
import org.elasticsearch.geo.geometry.Line;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiLine;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.MultiPolygon;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.geo.geometry.Rectangle;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.util.Objects;

/**
 * Wrapper class to represent a GeoShape in SQL
 *
 * It is required to override the XContent serialization. The ShapeBuilder serializes using GeoJSON by default,
 * but in SQL we need the serialization to be WKT-based.
 */
public class GeoShape implements ToXContentFragment, NamedWriteable {

    public static final String NAME = "geo";

    private final ShapeBuilder<?, ?, ?> shapeBuilder;

    public GeoShape(double lon, double lat) {
        shapeBuilder = new PointBuilder(lon, lat);
    }

    public GeoShape(Object value) throws IOException {
        shapeBuilder = ShapeParser.parse(value);
    }

    public GeoShape(StreamInput in) throws IOException {
        shapeBuilder = ShapeParser.parse(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(shapeBuilder.toWKT());
    }

    @Override
    public String toString() {
        return shapeBuilder.toWKT();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(shapeBuilder.toWKT());
    }

    public Geometry toGeometry() {
        return shapeBuilder.buildGeometry();
    }

    public Point firstPoint() {
        return shapeBuilder.buildGeometry().visit(new GeometryVisitor<Point>() {
            @Override
            public Point visit(Circle circle) {
                return new Point(circle.getLat(), circle.getLon(), circle.hasAlt() ? circle.getAlt() : Double.NaN);
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
                    return new Point(line.getLat(0), line.getLon(0), line.hasAlt() ? line.getAlt(0) :  Double.NaN);
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
                return new Point(rectangle.getMinLat(), rectangle.getMinLon(), rectangle.getMinAlt());
            }
        });
    }

    public Double getX() {
        Point firstPoint = firstPoint();
        if (firstPoint != null) {
            return firstPoint.getLon();
        } else {
            return null;
        }
    }

    public Double getY() {
        Point firstPoint = firstPoint();
        if (firstPoint != null) {
            return firstPoint.getLat();
        } else {
            return null;
        }
    }

    public String getGeometryType() {
        return toGeometry().type().name();
    }

    public static double distance(GeoShape shape1, GeoShape shape2) {
        if (shape1.shapeBuilder instanceof PointBuilder == false) {
            throw new SqlIllegalArgumentException("distance calculation is only supported for points; received [{}]", shape1);
        }
        if (shape2.shapeBuilder instanceof PointBuilder == false) {
            throw new SqlIllegalArgumentException("distance calculation is only supported for points; received [{}]", shape2);
        }
        double srcLat = ((PointBuilder) shape1.shapeBuilder).latitude();
        double srcLon = ((PointBuilder) shape1.shapeBuilder).longitude();
        double dstLat = ((PointBuilder) shape2.shapeBuilder).latitude();
        double dstLon = ((PointBuilder) shape2.shapeBuilder).longitude();
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
        return shapeBuilder.equals(geoShape.shapeBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shapeBuilder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

}
