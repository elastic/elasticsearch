/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.geometry.Circle;
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

public class SpatialEnvelopeVisitor implements GeometryVisitor<Rectangle, RuntimeException> {

    private double minX = Double.POSITIVE_INFINITY;
    private double minY = Double.POSITIVE_INFINITY;
    private double maxX = Double.NEGATIVE_INFINITY;
    private double maxY = Double.NEGATIVE_INFINITY;

    @Override
    public Rectangle visit(Circle circle) throws RuntimeException {
        // TODO: Support circle, if given CRS (needed for radius to x/y coordinate transformation)
        throw new UnsupportedOperationException("Circle is not supported");
    }

    @Override
    public Rectangle visit(GeometryCollection<?> collection) throws RuntimeException {
        collection.forEach(geometry -> {
            Rectangle envelope = geometry.visit(this);
            if (envelope != null) {
                minX = Math.min(minX, envelope.getMinX());
                minY = Math.min(minY, envelope.getMinY());
                maxX = Math.max(maxX, envelope.getMaxX());
                maxY = Math.max(maxY, envelope.getMaxY());
            }
        });
        return new Rectangle(minX, maxX, maxY, minY);
    }

    @Override
    public Rectangle visit(Line line) throws RuntimeException {
        for (int i = 0; i < line.length(); i++) {
            double x = line.getX(i);
            double y = line.getY(i);
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
        }
        return new Rectangle(minX, maxX, maxY, minY);
    }

    @Override
    public Rectangle visit(LinearRing ring) throws RuntimeException {
        for (int i = 0; i < ring.length(); i++) {
            double x = ring.getX(i);
            double y = ring.getY(i);
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
        }
        return new Rectangle(minX, maxX, maxY, minY);
    }

    @Override
    public Rectangle visit(MultiLine multiLine) throws RuntimeException {
        multiLine.forEach(line -> {
            Rectangle envelope = line.visit(this);
            if (envelope != null) {
                minX = Math.min(minX, envelope.getMinX());
                minY = Math.min(minY, envelope.getMinY());
                maxX = Math.max(maxX, envelope.getMaxX());
                maxY = Math.max(maxY, envelope.getMaxY());
            }
        });
        return new Rectangle(minX, maxX, maxY, minY);
    }

    @Override
    public Rectangle visit(MultiPoint multiPoint) throws RuntimeException {
        for (int i = 0; i < multiPoint.size(); i++) {
            Point point = multiPoint.get(i);
            minX = Math.min(minX, point.getX());
            minY = Math.min(minY, point.getY());
            maxX = Math.max(maxX, point.getX());
            maxY = Math.max(maxY, point.getY());
        }
        return new Rectangle(minX, maxX, maxY, minY);
    }

    @Override
    public Rectangle visit(MultiPolygon multiPolygon) throws RuntimeException {
        multiPolygon.forEach(polygon -> {
            Rectangle envelope = polygon.visit(this);
            if (envelope != null) {
                minX = Math.min(minX, envelope.getMinX());
                minY = Math.min(minY, envelope.getMinY());
                maxX = Math.max(maxX, envelope.getMaxX());
                maxY = Math.max(maxY, envelope.getMaxY());
            }
        });
        return new Rectangle(minX, maxX, maxY, minY);
    }

    @Override
    public Rectangle visit(Point point) throws RuntimeException {
        minX = Math.min(minX, point.getX());
        minY = Math.min(minY, point.getY());
        maxX = Math.max(maxX, point.getX());
        maxY = Math.max(maxY, point.getY());
        return new Rectangle(minX, maxX, maxY, minY);
    }

    @Override
    public Rectangle visit(Polygon polygon) throws RuntimeException {
        // TODO: Should we visit the holes also?
        return visit(polygon.getPolygon());
    }

    @Override
    public Rectangle visit(Rectangle rectangle) throws RuntimeException {
        minX = Math.min(minX, rectangle.getMinX());
        minY = Math.min(minY, rectangle.getMinY());
        maxX = Math.max(maxX, rectangle.getMaxX());
        maxY = Math.max(maxY, rectangle.getMaxY());
        return new Rectangle(minX, maxX, maxY, minY);
    }
}
