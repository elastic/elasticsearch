/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Converts Elasticsearch {@link Geometry} objects, such as those reconstructed from shape doc values, directly into
 * JTS geometries, copying the coordinates without an intermediate WKB or WKT encoding. Only two dimensions are
 * kept, which is all that doc values hold.
 */
final class JtsGeometryConverter implements GeometryVisitor<org.locationtech.jts.geom.Geometry, RuntimeException> {

    private final GeometryFactory factory;

    JtsGeometryConverter(GeometryFactory factory) {
        this.factory = factory;
    }

    org.locationtech.jts.geom.Geometry toJts(Geometry geometry) {
        return geometry.visit(this);
    }

    @Override
    public org.locationtech.jts.geom.Geometry visit(Circle circle) {
        throw new IllegalArgumentException("circles are not supported");
    }

    @Override
    public org.locationtech.jts.geom.Geometry visit(GeometryCollection<?> collection) {
        org.locationtech.jts.geom.Geometry[] geometries = new org.locationtech.jts.geom.Geometry[collection.size()];
        for (int i = 0; i < geometries.length; i++) {
            geometries[i] = collection.get(i).visit(this);
        }
        return factory.createGeometryCollection(geometries);
    }

    @Override
    public org.locationtech.jts.geom.Geometry visit(Line line) {
        return factory.createLineString(coordinates(line));
    }

    @Override
    public org.locationtech.jts.geom.Geometry visit(LinearRing ring) {
        return factory.createLinearRing(coordinates(ring));
    }

    @Override
    public org.locationtech.jts.geom.Geometry visit(MultiLine multiLine) {
        org.locationtech.jts.geom.LineString[] lines = new org.locationtech.jts.geom.LineString[multiLine.size()];
        for (int i = 0; i < lines.length; i++) {
            lines[i] = factory.createLineString(coordinates(multiLine.get(i)));
        }
        return factory.createMultiLineString(lines);
    }

    @Override
    public org.locationtech.jts.geom.Geometry visit(MultiPoint multiPoint) {
        org.locationtech.jts.geom.Point[] points = new org.locationtech.jts.geom.Point[multiPoint.size()];
        for (int i = 0; i < points.length; i++) {
            points[i] = point(multiPoint.get(i));
        }
        return factory.createMultiPoint(points);
    }

    @Override
    public org.locationtech.jts.geom.Geometry visit(MultiPolygon multiPolygon) {
        org.locationtech.jts.geom.Polygon[] polygons = new org.locationtech.jts.geom.Polygon[multiPolygon.size()];
        for (int i = 0; i < polygons.length; i++) {
            polygons[i] = polygon(multiPolygon.get(i));
        }
        return factory.createMultiPolygon(polygons);
    }

    @Override
    public org.locationtech.jts.geom.Geometry visit(Point point) {
        return point(point);
    }

    @Override
    public org.locationtech.jts.geom.Geometry visit(Polygon polygon) {
        return polygon(polygon);
    }

    @Override
    public org.locationtech.jts.geom.Geometry visit(Rectangle rectangle) {
        if (rectangle.isEmpty()) {
            return factory.createPolygon();
        }
        Coordinate bottomLeft = new Coordinate(rectangle.getMinX(), rectangle.getMinY());
        return factory.createPolygon(
            new Coordinate[] {
                bottomLeft,
                new Coordinate(rectangle.getMaxX(), rectangle.getMinY()),
                new Coordinate(rectangle.getMaxX(), rectangle.getMaxY()),
                new Coordinate(rectangle.getMinX(), rectangle.getMaxY()),
                bottomLeft.copy() }
        );
    }

    private org.locationtech.jts.geom.Point point(Point point) {
        return point.isEmpty() ? factory.createPoint() : factory.createPoint(new Coordinate(point.getX(), point.getY()));
    }

    private org.locationtech.jts.geom.Polygon polygon(Polygon polygon) {
        if (polygon.isEmpty()) {
            return factory.createPolygon();
        }
        org.locationtech.jts.geom.LinearRing shell = factory.createLinearRing(coordinates(polygon.getPolygon()));
        org.locationtech.jts.geom.LinearRing[] holes = new org.locationtech.jts.geom.LinearRing[polygon.getNumberOfHoles()];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = factory.createLinearRing(coordinates(polygon.getHole(i)));
        }
        return factory.createPolygon(shell, holes);
    }

    private static Coordinate[] coordinates(Line line) {
        double[] xs = line.getX();
        double[] ys = line.getY();
        Coordinate[] coordinates = new Coordinate[xs.length];
        for (int i = 0; i < coordinates.length; i++) {
            coordinates[i] = new Coordinate(xs[i], ys[i]);
        }
        return coordinates;
    }
}
