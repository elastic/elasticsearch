/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

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

import java.util.Optional;

public class SpatialEnvelopeVisitor implements GeometryVisitor<Boolean, RuntimeException> {

    private double minX = Double.POSITIVE_INFINITY;
    private double minY = Double.POSITIVE_INFINITY;
    private double maxX = Double.NEGATIVE_INFINITY;
    private double maxY = Double.NEGATIVE_INFINITY;

    public static Optional<Rectangle> visit(Geometry geometry) {
        var visitor = new SpatialEnvelopeVisitor();
        if (geometry.visit(visitor)) {
            return Optional.of(visitor.getResult());
        }
        return Optional.empty();
    }

    public Rectangle getResult() {
        return new Rectangle(minX, maxX, maxY, minY);
    }

    private boolean isValid() {
        return minX != Double.POSITIVE_INFINITY;
    }

    @Override
    public Boolean visit(Circle circle) throws RuntimeException {
        // TODO: Support circle better, if given CRS (needed for radius to x/y coordinate transformation)
        minX = Math.min(minX, circle.getX() - circle.getRadiusMeters());
        minY = Math.min(minY, circle.getY() - circle.getRadiusMeters());
        maxY = Math.max(maxY, circle.getY() + circle.getRadiusMeters());
        maxX = Math.max(maxX, circle.getX() + circle.getRadiusMeters());
        return true;
    }

    @Override
    public Boolean visit(GeometryCollection<?> collection) throws RuntimeException {
        collection.forEach(geometry -> geometry.visit(this));
        return isValid();
    }

    @Override
    public Boolean visit(Line line) throws RuntimeException {
        for (int i = 0; i < line.length(); i++) {
            double x = line.getX(i);
            double y = line.getY(i);
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
        }
        return isValid();
    }

    @Override
    public Boolean visit(LinearRing ring) throws RuntimeException {
        for (int i = 0; i < ring.length(); i++) {
            double x = ring.getX(i);
            double y = ring.getY(i);
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
        }
        return isValid();
    }

    @Override
    public Boolean visit(MultiLine multiLine) throws RuntimeException {
        multiLine.forEach(line -> line.visit(this));
        return isValid();
    }

    @Override
    public Boolean visit(MultiPoint multiPoint) throws RuntimeException {
        for (int i = 0; i < multiPoint.size(); i++) {
            Point point = multiPoint.get(i);
            minX = Math.min(minX, point.getX());
            minY = Math.min(minY, point.getY());
            maxX = Math.max(maxX, point.getX());
            maxY = Math.max(maxY, point.getY());
        }
        return isValid();
    }

    @Override
    public Boolean visit(MultiPolygon multiPolygon) throws RuntimeException {
        multiPolygon.forEach(polygon -> polygon.visit(this));
        return isValid();
    }

    @Override
    public Boolean visit(Point point) throws RuntimeException {
        minX = Math.min(minX, point.getX());
        minY = Math.min(minY, point.getY());
        maxX = Math.max(maxX, point.getX());
        maxY = Math.max(maxY, point.getY());
        return isValid();
    }

    @Override
    public Boolean visit(Polygon polygon) throws RuntimeException {
        visit(polygon.getPolygon());
        for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
            visit(polygon.getHole(i));
        }
        return isValid();
    }

    @Override
    public Boolean visit(Rectangle rectangle) throws RuntimeException {
        minX = Math.min(minX, rectangle.getMinX());
        minY = Math.min(minY, rectangle.getMinY());
        maxX = Math.max(maxX, rectangle.getMaxX());
        maxY = Math.max(maxY, rectangle.getMaxY());
        return isValid();
    }
}
