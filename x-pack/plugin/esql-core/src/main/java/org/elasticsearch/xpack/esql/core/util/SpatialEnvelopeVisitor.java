/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

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

    private final PointVisitor pointVisitor;

    public SpatialEnvelopeVisitor(PointVisitor pointVisitor) {
        this.pointVisitor = pointVisitor;
    }

    /**
     * Determine the BBOX without considering the CRS or wrapping of the longitude.
     * Note that incoming BBOX's that do cross the dateline (minx>maxx) will be treated as invalid.
     */
    public static Optional<Rectangle> visit(Geometry geometry) {
        var visitor = new SpatialEnvelopeVisitor(new CartesianPointVisitor());
        if (geometry.visit(visitor)) {
            return Optional.of(visitor.getResult());
        }
        return Optional.empty();
    }

    /**
     * Determine the BBOX assuming the CRS is geographic (eg WGS84) and optionally wrapping the longitude around the dateline.
     */
    public static Optional<Rectangle> visit(Geometry geometry, boolean wrapLongitude) {
        var visitor = new SpatialEnvelopeVisitor(new GeoPointVisitor(wrapLongitude));
        if (geometry.visit(visitor)) {
            return Optional.of(visitor.getResult());
        }
        return Optional.empty();
    }

    public Rectangle getResult() {
        return pointVisitor.getResult();
    }

    private interface PointVisitor {
        void visitPoint(double x, double y);

        void visitRectangle(double minX, double maxX, double maxY, double minY);

        boolean isValid();

        Rectangle getResult();
    }

    static class CartesianPointVisitor implements PointVisitor {
        private double minX = Double.POSITIVE_INFINITY;
        private double minY = Double.POSITIVE_INFINITY;
        private double maxX = Double.NEGATIVE_INFINITY;
        private double maxY = Double.NEGATIVE_INFINITY;

        @Override
        public void visitPoint(double x, double y) {
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
        }

        @Override
        public void visitRectangle(double minX, double maxX, double maxY, double minY) {
            if (minX > maxX) {
                throw new IllegalArgumentException("Invalid cartesian rectangle: minX > maxX");
            }
            this.minX = Math.min(this.minX, minX);
            this.minY = Math.min(this.minY, minY);
            this.maxX = Math.max(this.maxX, maxX);
            this.maxY = Math.max(this.maxY, maxY);
        }

        @Override
        public boolean isValid() {
            return minY != Double.POSITIVE_INFINITY;
        }

        @Override
        public Rectangle getResult() {
            return new Rectangle(minX, maxX, maxY, minY);
        }
    }

    static class GeoPointVisitor implements PointVisitor {
        private double minY = Double.POSITIVE_INFINITY;
        private double maxY = Double.NEGATIVE_INFINITY;
        private double minNegX = Double.POSITIVE_INFINITY;
        private double maxNegX = Double.NEGATIVE_INFINITY;
        private double minPosX = Double.POSITIVE_INFINITY;
        private double maxPosX = Double.NEGATIVE_INFINITY;

        private final boolean wrapLongitude;

        GeoPointVisitor(boolean wrapLongitude) {
            this.wrapLongitude = wrapLongitude;
        }

        @Override
        public void visitPoint(double x, double y) {
            minY = Math.min(minY, y);
            maxY = Math.max(maxY, y);
            visitLongitude(x);
        }

        @Override
        public void visitRectangle(double minX, double maxX, double maxY, double minY) {
            this.minY = Math.min(this.minY, minY);
            this.maxY = Math.max(this.maxY, maxY);
            visitLongitude(minX);
            visitLongitude(maxX);
        }

        private void visitLongitude(double x) {
            if (x >= 0) {
                minPosX = Math.min(minPosX, x);
                maxPosX = Math.max(maxPosX, x);
            } else {
                minNegX = Math.min(minNegX, x);
                maxNegX = Math.max(maxNegX, x);
            }
        }

        @Override
        public boolean isValid() {
            return minY != Double.POSITIVE_INFINITY;
        }

        @Override
        public Rectangle getResult() {
            if (Double.isInfinite(maxY)) {
                return null;
            } else if (Double.isInfinite(minPosX)) {
                return new Rectangle(minNegX, maxNegX, maxY, minY);
            } else if (Double.isInfinite(minNegX)) {
                return new Rectangle(minPosX, maxPosX, maxY, minY);
            } else if (wrapLongitude) {
                double unwrappedWidth = maxPosX - minNegX;
                double wrappedWidth = (180 - minPosX) - (-180 - maxNegX);
                if (unwrappedWidth <= wrappedWidth) {
                    return new Rectangle(minNegX, maxPosX, maxY, minY);
                } else {
                    return new Rectangle(minPosX, maxNegX, maxY, minY);
                }
            } else {
                return new Rectangle(minNegX, maxPosX, maxY, minY);
            }
        }
    }

    private boolean isValid() {
        return pointVisitor.isValid();
    }

    @Override
    public Boolean visit(Circle circle) throws RuntimeException {
        // TODO: Support circle, if given CRS (needed for radius to x/y coordinate transformation)
        throw new UnsupportedOperationException("Circle is not supported");
    }

    @Override
    public Boolean visit(GeometryCollection<?> collection) throws RuntimeException {
        collection.forEach(geometry -> geometry.visit(this));
        return isValid();
    }

    @Override
    public Boolean visit(Line line) throws RuntimeException {
        for (int i = 0; i < line.length(); i++) {
            pointVisitor.visitPoint(line.getX(i), line.getY(i));
        }
        return isValid();
    }

    @Override
    public Boolean visit(LinearRing ring) throws RuntimeException {
        for (int i = 0; i < ring.length(); i++) {
            pointVisitor.visitPoint(ring.getX(i), ring.getY(i));
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
            visit(multiPoint.get(i));
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
        pointVisitor.visitPoint(point.getX(), point.getY());
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
        pointVisitor.visitRectangle(rectangle.getMinX(), rectangle.getMaxX(), rectangle.getMaxY(), rectangle.getMinY());
        return isValid();
    }
}
