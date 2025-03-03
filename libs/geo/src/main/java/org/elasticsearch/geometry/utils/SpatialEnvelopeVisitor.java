/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry.utils;

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

import java.util.Locale;
import java.util.Optional;

/**
 * This visitor is designed to determine the spatial envelope (or BBOX or MBR) of a potentially complex geometry.
 * It has two modes:
 * <ul>
 *     <li>
 *         Cartesian mode: The envelope is determined by the minimum and maximum x/y coordinates.
 *         Incoming BBOX geometries with minX &gt; maxX are treated as invalid.
 *         Resulting BBOX geometries will always have minX &lt;= maxX.
 *     </li>
 *     <li>
 *         Geographic mode: The envelope is determined by the minimum and maximum x/y coordinates,
 *         considering the possibility of wrapping the longitude around the dateline.
 *         A bounding box can be determined either by wrapping the longitude around the dateline or not,
 *         and the smaller bounding box is chosen. It is possible to disable the wrapping of the longitude.
 * </ul>
 * Usage of this is as simple as:
 * <code>
 *     Optional&lt;Rectangle&gt; bbox = SpatialEnvelopeVisitor.visit(geometry);
 *     if (bbox.isPresent()) {
 *         Rectangle envelope = bbox.get();
 *         // Do stuff with the envelope
 *     }
 * </code>
 * It is also possible to create the inner <code>PointVisitor</code> separately, as well as use the visitor for multiple geometries.
 * <code>
 *     PointVisitor pointVisitor = new CartesianPointVisitor();
 *     SpatialEnvelopeVisitor visitor = new SpatialEnvelopeVisitor(pointVisitor);
 *     for (Geometry geometry : geometries) {
 *         geometry.visit(visitor);
 *     }
 *     if (visitor.isValid()) {
 *         Rectangle envelope = visitor.getResult();
 *         // Do stuff with the envelope
 *     }
 * </code>
 * Code that wishes to modify the behaviour of the visitor can implement the <code>PointVisitor</code> interface,
 * or extend the existing implementations.
 */
public class SpatialEnvelopeVisitor implements GeometryVisitor<Boolean, RuntimeException> {

    private final PointVisitor pointVisitor;

    public SpatialEnvelopeVisitor(PointVisitor pointVisitor) {
        this.pointVisitor = pointVisitor;
    }

    /**
     * Determine the BBOX without considering the CRS or wrapping of the longitude.
     * Note that incoming BBOX's that do cross the dateline (minx>maxx) will be treated as invalid.
     */
    public static Optional<Rectangle> visitCartesian(Geometry geometry) {
        var visitor = new SpatialEnvelopeVisitor(new CartesianPointVisitor());
        if (geometry.visit(visitor)) {
            return Optional.of(visitor.getResult());
        }
        return Optional.empty();
    }

    public enum WrapLongitude {
        NO_WRAP,
        WRAP
    }

    /**
     * Determine the BBOX assuming the CRS is geographic (eg WGS84) and optionally wrapping the longitude around the dateline.
     */
    public static Optional<Rectangle> visitGeo(Geometry geometry, WrapLongitude wrapLongitude) {
        var visitor = new SpatialEnvelopeVisitor(new GeoPointVisitor(wrapLongitude));
        if (geometry.visit(visitor)) {
            return Optional.of(visitor.getResult());
        }
        return Optional.empty();
    }

    public Rectangle getResult() {
        return pointVisitor.getResult();
    }

    /**
     * Visitor for visiting points and rectangles. This is where the actual envelope calculation happens.
     * There are two implementations, one for cartesian coordinates and one for geographic coordinates.
     * The latter can optionally wrap the longitude around the dateline.
     */
    public interface PointVisitor {
        void visitPoint(double x, double y);

        void visitRectangle(double minX, double maxX, double maxY, double minY);

        boolean isValid();

        Rectangle getResult();

        /** To allow for memory optimizations through object reuse, the visitor can be reset to its initial state. */
        void reset();
    }

    /**
     * The cartesian point visitor determines the envelope by the minimum and maximum x/y coordinates.
     * It also disallows invalid rectangles where minX > maxX.
     */
    public static class CartesianPointVisitor implements PointVisitor {
        private double minX = Double.POSITIVE_INFINITY;
        private double maxX = Double.NEGATIVE_INFINITY;
        private double maxY = Double.NEGATIVE_INFINITY;
        private double minY = Double.POSITIVE_INFINITY;

        public double getMinX() {
            return minX;
        }

        public double getMaxX() {
            return maxX;
        }

        public double getMaxY() {
            return maxY;
        }

        public double getMinY() {
            return minY;
        }

        @Override
        public void visitPoint(double x, double y) {
            minX = Math.min(minX, x);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
            minY = Math.min(minY, y);
        }

        @Override
        public void visitRectangle(double minX, double maxX, double maxY, double minY) {
            if (minX > maxX) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid cartesian rectangle: minX (%s) > maxX (%s)", minX, maxX)
                );
            }
            this.minX = Math.min(this.minX, minX);
            this.maxX = Math.max(this.maxX, maxX);
            this.maxY = Math.max(this.maxY, maxY);
            this.minY = Math.min(this.minY, minY);
        }

        @Override
        public boolean isValid() {
            return minY != Double.POSITIVE_INFINITY;
        }

        @Override
        public Rectangle getResult() {
            return new Rectangle(minX, maxX, maxY, minY);
        }

        @Override
        public void reset() {
            minX = Double.POSITIVE_INFINITY;
            maxX = Double.NEGATIVE_INFINITY;
            maxY = Double.NEGATIVE_INFINITY;
            minY = Double.POSITIVE_INFINITY;
        }
    }

    /**
     * The geographic point visitor determines the envelope by the minimum and maximum x/y coordinates,
     * while allowing for wrapping the longitude around the dateline.
     * When longitude wrapping is enabled, the visitor will determine the smallest bounding box between the two choices:
     * <ul>
     *     <li>Wrapping around the front of the earth, in which case the result will have minx &lt; maxx</li>
     *     <li>Wrapping around the back of the earth, crossing the dateline, in which case the result will have minx &gt; maxx</li>
     * </ul>
     */
    public static class GeoPointVisitor implements PointVisitor {
        protected double top = Double.NEGATIVE_INFINITY;
        protected double bottom = Double.POSITIVE_INFINITY;
        protected double negLeft = Double.POSITIVE_INFINITY;
        protected double negRight = Double.NEGATIVE_INFINITY;
        protected double posLeft = Double.POSITIVE_INFINITY;
        protected double posRight = Double.NEGATIVE_INFINITY;

        private final WrapLongitude wrapLongitude;

        public GeoPointVisitor(WrapLongitude wrapLongitude) {
            this.wrapLongitude = wrapLongitude;
        }

        public double getTop() {
            return top;
        }

        public double getBottom() {
            return bottom;
        }

        public double getNegLeft() {
            return negLeft;
        }

        public double getNegRight() {
            return negRight;
        }

        public double getPosLeft() {
            return posLeft;
        }

        public double getPosRight() {
            return posRight;
        }

        @Override
        public void visitPoint(double x, double y) {
            bottom = Math.min(bottom, y);
            top = Math.max(top, y);
            visitLongitude(x);
        }

        @Override
        public void visitRectangle(double minX, double maxX, double maxY, double minY) {
            // TODO: Fix bug with rectangle crossing the dateline (see Extent.addRectangle for correct behaviour)
            this.bottom = Math.min(this.bottom, minY);
            this.top = Math.max(this.top, maxY);
            visitLongitude(minX);
            visitLongitude(maxX);
        }

        private void visitLongitude(double x) {
            if (x >= 0) {
                posLeft = Math.min(posLeft, x);
                posRight = Math.max(posRight, x);
            } else {
                negLeft = Math.min(negLeft, x);
                negRight = Math.max(negRight, x);
            }
        }

        @Override
        public boolean isValid() {
            return bottom != Double.POSITIVE_INFINITY;
        }

        @Override
        public Rectangle getResult() {
            return getResult(top, bottom, negLeft, negRight, posLeft, posRight, wrapLongitude);
        }

        @Override
        public void reset() {
            bottom = Double.POSITIVE_INFINITY;
            top = Double.NEGATIVE_INFINITY;
            negLeft = Double.POSITIVE_INFINITY;
            negRight = Double.NEGATIVE_INFINITY;
            posLeft = Double.POSITIVE_INFINITY;
            posRight = Double.NEGATIVE_INFINITY;
        }

        public static Rectangle getResult(
            double top,
            double bottom,
            double negLeft,
            double negRight,
            double posLeft,
            double posRight,
            WrapLongitude wrapLongitude
        ) {
            assert Double.isFinite(top);
            if (posRight == Double.NEGATIVE_INFINITY) {
                return new Rectangle(negLeft, negRight, top, bottom);
            } else if (negLeft == Double.POSITIVE_INFINITY) {
                return new Rectangle(posLeft, posRight, top, bottom);
            } else {
                return switch (wrapLongitude) {
                    case NO_WRAP -> new Rectangle(negLeft, posRight, top, bottom);
                    case WRAP -> maybeWrap(top, bottom, negLeft, negRight, posLeft, posRight);
                };
            }
        }

        private static Rectangle maybeWrap(double top, double bottom, double negLeft, double negRight, double posLeft, double posRight) {
            double unwrappedWidth = posRight - negLeft;
            double wrappedWidth = 360 + negRight - posLeft;
            return unwrappedWidth <= wrappedWidth
                ? new Rectangle(negLeft, posRight, top, bottom)
                : new Rectangle(posLeft, negRight, top, bottom);
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
