/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;

import java.util.HashMap;
import java.util.Map;
import java.util.function.DoubleUnaryOperator;

import static org.apache.lucene.geo.GeoUtils.lineCrossesLine;
import static org.apache.lucene.geo.GeoUtils.lineCrossesLineWithBoundary;

/**
 * Utility class that generates H3 bins projected on the cartesian plane (equirectangular projection).
 */
public final class H3CartesianUtil {

    private static final long[] NORTH = new long[16];
    private static final long[] SOUTH = new long[16];
    static {
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            NORTH[res] = H3.geoToH3(90, 0, res);
            SOUTH[res] = H3.geoToH3(-90, 0, res);
        }
    }
    // we cache the first two levels and polar polygons
    private static final Map<Long, Component2D> CACHED_COMPONENTS = new HashMap<>();
    static {
        for (long res0Cell : H3.getLongRes0Cells()) {
            CACHED_COMPONENTS.put(res0Cell, getComponent(res0Cell));
            for (long h3 : H3.h3ToChildren(res0Cell)) {
                CACHED_COMPONENTS.put(h3, getComponent(h3));
            }
        }
        for (int res = 2; res <= H3.MAX_H3_RES; res++) {
            CACHED_COMPONENTS.put(NORTH[res], getComponent(NORTH[res]));
            CACHED_COMPONENTS.put(SOUTH[res], getComponent(SOUTH[res]));
        }
    }

    private static final double[] NORTH_BOUND = new double[16];
    private static final double[] SOUTH_BOUND = new double[16];
    static {
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            NORTH_BOUND[res] = CACHED_COMPONENTS.get(NORTH[res]).getMinY();
            SOUTH_BOUND[res] = CACHED_COMPONENTS.get(SOUTH[res]).getMaxY();
        }
    }

    /** For the given resolution, it returns the maximum latitude of the h3 bin containing the south pole */
    public static double getSouthPolarBound(int resolution) {
        return SOUTH_BOUND[resolution];
    }

    /** For the given resolution, it returns the minimum latitude of the h3 bin containing the north pole */
    public static double getNorthPolarBound(int resolution) {
        return NORTH_BOUND[resolution];
    }

    /** Return the {@link LatLonGeometry} representing the provided H3 bin */
    public static LatLonGeometry getLatLonGeometry(long h3) {
        return new H3CartesianGeometry(h3);
    }

    /** Return the {@link Component2D} representing the provided H3 bin */
    public static Component2D getComponent(long h3) {
        final Component2D component2D = CACHED_COMPONENTS.get(h3);
        return component2D == null ? new H3CartesianComponent(h3) : component2D;
    }

    /** Return the bounding box of the provided H3 bin */
    public static org.elasticsearch.geometry.Rectangle toBoundingBox(long h3) {
        final CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        double minLat = Double.POSITIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        double maxLon = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < boundary.numPoints(); i++) {
            final LatLng latLng = boundary.getLatLon(i);
            minLat = Math.min(minLat, latLng.getLatDeg());
            minLon = Math.min(minLon, latLng.getLonDeg());
            maxLat = Math.max(maxLat, latLng.getLatDeg());
            maxLon = Math.max(maxLon, latLng.getLonDeg());
        }
        final int res = H3.getResolution(h3);
        if (h3 == NORTH[res]) {
            return new org.elasticsearch.geometry.Rectangle(-180d, 180d, 90d, minLat);
        } else if (h3 == SOUTH[res]) {
            return new org.elasticsearch.geometry.Rectangle(-180d, 180d, maxLat, -90d);
        } else {
            return new org.elasticsearch.geometry.Rectangle(minLon, maxLon, maxLat, minLat);
        }
    }

    // pkg private, used for testing
    static Geometry getGeometry(long h3) {
        final H3CartesianComponent component2D = new H3CartesianComponent(h3);
        final Polygon polygon = new Polygon(new LinearRing(component2D.xs, component2D.ys));
        if (component2D.crossesDateline) {
            final Geometry geometry = GeometryNormalizer.apply(Orientation.CCW, polygon);
            if (geometry instanceof Polygon) {
                // there is a bug on the code that breaks polygons across the dateline
                // when polygon is close to the pole (I think) so we need to try again
                return GeometryNormalizer.apply(Orientation.CW, polygon);
            }
            return geometry;
        } else {
            return polygon;
        }

    }

    private static class H3CartesianGeometry extends LatLonGeometry {

        private final long h3;

        H3CartesianGeometry(long h3) {
            this.h3 = h3;
        }

        @Override
        protected Component2D toComponent2D() {
            return new H3CartesianComponent(h3);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            H3CartesianGeometry that = (H3CartesianGeometry) o;
            return h3 == that.h3;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(h3);
        }
    }

    private static class H3CartesianComponent implements Component2D {

        private final double[] xs, ys;
        private final double minX, maxX, minY, maxY;
        private final boolean crossesDateline;

        H3CartesianComponent(long h3) {
            final int resolution = H3.getResolution(h3);
            final double pole = NORTH[resolution] == h3 ? 90d : SOUTH[resolution] == h3 ? -90d : Double.NaN;
            final CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3);
            double minX = Double.POSITIVE_INFINITY;
            double maxX = Double.NEGATIVE_INFINITY;
            double minY = Double.POSITIVE_INFINITY;
            double maxY = Double.NEGATIVE_INFINITY;
            if (Double.isNaN(pole)) {
                this.xs = new double[cellBoundary.numPoints() + 1];
                this.ys = new double[cellBoundary.numPoints() + 1];
            } else {
                this.xs = new double[cellBoundary.numPoints() + 5];
                this.ys = new double[cellBoundary.numPoints() + 5];
            }
            for (int i = 0; i < cellBoundary.numPoints(); i++) {
                final LatLng latLng = cellBoundary.getLatLon(i);
                this.xs[i] = latLng.getLonDeg();
                this.ys[i] = latLng.getLatDeg();
                minX = Math.min(minX, xs[i]);
                maxX = Math.max(maxX, xs[i]);
                minY = Math.min(minY, ys[i]);
                maxY = Math.max(maxY, ys[i]);
            }
            if (Double.isNaN(pole)) {
                xs[cellBoundary.numPoints()] = xs[0];
                ys[cellBoundary.numPoints()] = ys[0];
                this.minX = minX;
                this.maxX = maxX;
                this.minY = minY;
                this.maxY = maxY;
                this.crossesDateline = maxX - minX > 180d;
            } else {
                closePolarComponent(cellBoundary.numPoints(), pole);
                this.minX = -180d;
                this.maxX = 180d;
                this.minY = Math.min(pole, minY);
                this.maxY = Math.max(pole, maxY);
                this.crossesDateline = false;
            }
        }

        private void closePolarComponent(int numBoundaryPoints, double pole) {
            sort(xs, ys, numBoundaryPoints);
            assert xs[0] > 0 != xs[numBoundaryPoints - 1] > 0 : "expected first and last element with different sign";
            final double y = datelineIntersectionLatitude(xs[0], ys[0], xs[numBoundaryPoints - 1], ys[numBoundaryPoints - 1]);
            this.xs[numBoundaryPoints] = 180d;
            this.ys[numBoundaryPoints] = y;
            this.xs[numBoundaryPoints + 1] = 180d;
            this.ys[numBoundaryPoints + 1] = pole;
            this.xs[numBoundaryPoints + 2] = -180d;
            this.ys[numBoundaryPoints + 2] = pole;
            this.xs[numBoundaryPoints + 3] = -180d;
            this.ys[numBoundaryPoints + 3] = y;
            this.xs[xs.length - 1] = this.xs[0];
            this.ys[xs.length - 1] = this.ys[0];
        }

        private static double datelineIntersectionLatitude(double x1, double y1, double x2, double y2) {
            final double t = (180d - normalize_lon(x1, true)) / (normalize_lon(x2, true) - normalize_lon(x1, true));
            if (t > 1 || t <= 0) {
                throw new IllegalArgumentException("expected value between 0 and 1, got " + t);
            }
            return y1 + t * (y2 - y1);
        }

        private static void sort(double[] xs, double[] ys, int length) {
            new IntroSorter() {
                int pivotPos = -1;

                @Override
                protected void swap(int i, int j) {
                    double tmp = xs[i];
                    xs[i] = xs[j];
                    xs[j] = tmp;
                    tmp = ys[i];
                    ys[i] = ys[j];
                    ys[j] = tmp;
                }

                @Override
                protected void setPivot(int i) {
                    pivotPos = i;
                }

                @Override
                protected int comparePivot(int j) {
                    // all xs are different
                    return Double.compare(xs[pivotPos], xs[j]);
                }
            }.sort(0, length);
        }

        @Override
        public double getMinX() {
            return crossesDateline ? -180d : minX;
        }

        @Override
        public double getMaxX() {
            return crossesDateline ? 180d : maxX;
        }

        @Override
        public double getMinY() {
            return minY;
        }

        @Override
        public double getMaxY() {
            return maxY;
        }

        @Override
        public boolean contains(double x, double y) {
            // fail fast if we're outside the bounding box
            if (Rectangle.containsPoint(y, x, getMinY(), getMaxY(), getMinX(), getMaxX()) == false) {
                return false;
            }
            boolean res = false;
            x = normalize_lon(x, crossesDateline);
            for (int i = 0; i < xs.length - 1; i++) {
                double x1 = normalize_lon(xs[i], crossesDateline);
                double x2 = normalize_lon(xs[i + 1], crossesDateline);
                double y1 = ys[i];
                double y2 = ys[i + 1];
                if (y == y1 && y == y2 || (y <= y1 && y >= y2) != (y >= y1 && y <= y2)) {
                    if ((x == x1 && x == x2)
                        || ((x <= x1 && x >= x2) != (x >= x1 && x <= x2) && GeoUtils.orient(x1, y1, x2, y2, x, y) == 0)) {
                        return true;
                    } else if (y1 > y != y2 > y) {
                        res ^= x < (x2 - x1) * (y - y1) / (y2 - y1) + x1;
                    }
                }
            }
            return res;
        }

        private static double normalize_lon(double lon, boolean dateline) {
            return dateline && lon < 0 ? lon + 360d : lon;
        }

        @Override
        public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
            if (Component2D.disjoint(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
            if (Component2D.within(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            int numCorners = numberOfCorners(minX, maxX, minY, maxY);
            if (numCorners == 4) {
                if (crossesBox(minX, maxX, minY, maxY, true)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                return PointValues.Relation.CELL_INSIDE_QUERY;
            } else if (numCorners == 0) {
                if (Component2D.containsPoint(xs[0], ys[0], minX, maxX, minY, maxY)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                if (crossesBox(minX, maxX, minY, maxY, true)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }

        @Override
        public boolean intersectsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            return contains(aX, aY) || contains(bX, bY) || crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, true);
        }

        @Override
        public boolean intersectsTriangle(
            double minX,
            double maxX,
            double minY,
            double maxY,
            double aX,
            double aY,
            double bX,
            double bY,
            double cX,
            double cY
        ) {
            if (Component2D.disjoint(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return false;
            }
            return contains(aX, aY)
                || contains(bX, bY)
                || contains(cX, cY)
                || Component2D.pointInTriangle(minX, maxX, minY, maxY, xs[0], ys[0], aX, aY, bX, bY, cX, cY)
                || crossesTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY, true);
        }

        @Override
        public boolean containsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            return (contains(aX, aY) && contains(bX, bY)
            // TODO: we might only want to report contains if we don't touch the H3 bin?
                && crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, false) == false);
        }

        @Override
        public boolean containsTriangle(
            double minX,
            double maxX,
            double minY,
            double maxY,
            double aX,
            double aY,
            double bX,
            double bY,
            double cX,
            double cY
        ) {
            return (contains(aX, aY) && contains(bX, bY) && contains(cX, cY)
            // TODO: we might only want to report contains if we don't touch the H3 bin?
                && crossesTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY, false) == false);

        }

        @Override
        public WithinRelation withinPoint(double x, double y) {
            return contains(x, y) ? WithinRelation.NOTWITHIN : WithinRelation.DISJOINT;
        }

        @Override
        public WithinRelation withinLine(
            double minX,
            double maxX,
            double minY,
            double maxY,
            double aX,
            double aY,
            boolean ab,
            double bX,
            double bY
        ) {
            if (Component2D.disjoint(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return WithinRelation.DISJOINT;
            }
            if (contains(aX, aY) || contains(bX, bY)) {
                return WithinRelation.NOTWITHIN;
            }
            if (ab && crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, true)) {
                return WithinRelation.NOTWITHIN;
            }
            return WithinRelation.DISJOINT;
        }

        @Override
        public WithinRelation withinTriangle(
            double minX,
            double maxX,
            double minY,
            double maxY,
            double aX,
            double aY,
            boolean ab,
            double bX,
            double bY,
            boolean bc,
            double cX,
            double cY,
            boolean ca
        ) {
            if (Component2D.disjoint(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return WithinRelation.DISJOINT;
            }

            // if any of the points is inside the polygon, the polygon cannot be within this indexed
            // shape because points belong to the original indexed shape.
            if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
                return WithinRelation.NOTWITHIN;
            }

            WithinRelation relation = WithinRelation.DISJOINT;
            // if any of the edges intersects an the edge belongs to the shape then it cannot be within.
            // if it only intersects edges that do not belong to the shape, then it is a candidate
            // we skip edges at the dateline to support shapes crossing it
            if (crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, true)) {
                if (ab) {
                    return WithinRelation.NOTWITHIN;
                } else {
                    relation = WithinRelation.CANDIDATE;
                }
            }

            if (crossesLine(minX, maxX, minY, maxY, bX, bY, cX, cY, true)) {
                if (bc) {
                    return WithinRelation.NOTWITHIN;
                } else {
                    relation = WithinRelation.CANDIDATE;
                }
            }
            if (crossesLine(minX, maxX, minY, maxY, cX, cY, aX, aY, true)) {
                if (ca) {
                    return WithinRelation.NOTWITHIN;
                } else {
                    relation = WithinRelation.CANDIDATE;
                }
            }

            // if any of the edges crosses and edge that does not belong to the shape
            // then it is a candidate for within
            if (relation == WithinRelation.CANDIDATE) {
                return WithinRelation.CANDIDATE;
            }

            // Check if shape is within the triangle
            if (Component2D.pointInTriangle(minX, maxX, minY, maxY, xs[0], ys[0], aX, aY, bX, bY, cX, cY)) {
                return WithinRelation.CANDIDATE;
            }
            return relation;
        }

        private boolean crossesBox(double minX, double maxX, double minY, double maxY, boolean includeBoundary) {
            if (crossesDateline) {
                return crossesBox(minX, maxX, minY, maxY, includeBoundary, lon -> lon < 0 ? lon + 360d : lon)
                    || crossesBox(minX, maxX, minY, maxY, includeBoundary, lon -> lon > 0 ? lon - 360d : lon);
            } else {
                return crossesBox(minX, maxX, minY, maxY, includeBoundary, DoubleUnaryOperator.identity());
            }
        }

        private boolean crossesBox(
            double minX,
            double maxX,
            double minY,
            double maxY,
            boolean includeBoundary,
            DoubleUnaryOperator normalizeLong
        ) {
            // we just have to cross one edge to answer the question, so we descend the tree and return when
            // we do.
            for (int i = 0; i < xs.length - 1; i++) {
                // we compute line intersections of every polygon edge with every box line.
                // if we find one, return true.
                // for each box line (AB):
                // for each poly line (CD):
                // intersects = orient(C,D,A) * orient(C,D,B) <= 0 && orient(A,B,C) * orient(A,B,D) <= 0
                double cy = ys[i];
                double dy = ys[i + 1];
                double cx = normalizeLong.applyAsDouble(xs[i]);
                double dx = normalizeLong.applyAsDouble(xs[i + 1]);

                // optimization: see if either end of the line segment is contained by the rectangle
                if (Rectangle.containsPoint(cy, cx, minY, maxY, minX, maxX) || Rectangle.containsPoint(dy, dx, minY, maxY, minX, maxX)) {
                    return true;
                }

                // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
                // if not, don't waste our time trying more complicated stuff
                boolean outside = (cy < minY && dy < minY)
                    || (cy > maxY && dy > maxY)
                    || (cx < minX && dx < minX)
                    || (cx > maxX && dx > maxX);

                if (outside == false) {
                    if (includeBoundary) {
                        if (lineCrossesLineWithBoundary(cx, cy, dx, dy, minX, minY, maxX, minY)
                            || lineCrossesLineWithBoundary(cx, cy, dx, dy, maxX, minY, maxX, maxY)
                            || lineCrossesLineWithBoundary(cx, cy, dx, dy, maxX, maxY, minX, maxY)
                            || lineCrossesLineWithBoundary(cx, cy, dx, dy, minX, maxY, minX, minY)) {
                            // include boundaries: ensures box edges that terminate on the polygon are included
                            return true;
                        }
                    } else {
                        if (lineCrossesLine(cx, cy, dx, dy, minX, minY, maxX, minY)
                            || lineCrossesLine(cx, cy, dx, dy, maxX, minY, maxX, maxY)
                            || lineCrossesLine(cx, cy, dx, dy, maxX, maxY, minX, maxY)
                            || lineCrossesLine(cx, cy, dx, dy, minX, maxY, minX, minY)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        private boolean crossesLine(
            double minX,
            double maxX,
            double minY,
            double maxY,
            double aX,
            double aY,
            double bX,
            double bY,
            boolean includeBoundary
        ) {
            if (crossesDateline) {
                return crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, includeBoundary, lon -> lon < 0 ? lon + 360d : lon)
                    || crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, includeBoundary, lon -> lon > 0 ? lon - 360d : lon);
            } else {
                return crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, includeBoundary, DoubleUnaryOperator.identity());
            }
        }

        private boolean crossesLine(
            double minX,
            double maxX,
            double minY,
            double maxY,
            double aX,
            double aY,
            double bX,
            double bY,
            boolean includeBoundary,
            DoubleUnaryOperator normalizeLong
        ) {
            if (Component2D.disjoint(getMinX(), getMaxX(), getMinY(), getMaxY(), minX, maxX, minY, maxY)) {
                return false;
            }
            for (int i = 0; i < xs.length - 1; i++) {
                double cy = ys[i];
                double dy = ys[i + 1];
                double cx = normalizeLong.applyAsDouble(xs[i]);
                double dx = normalizeLong.applyAsDouble(xs[i + 1]);
                if (includeBoundary) {
                    if (GeoUtils.lineCrossesLineWithBoundary(cx, cy, dx, dy, aX, aY, bX, bY)) {
                        return true;
                    }
                } else {
                    if (GeoUtils.lineCrossesLine(cx, cy, dx, dy, aX, aY, bX, bY)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private int numberOfCorners(double minX, double maxX, double minY, double maxY) {
            int containsCount = 0;
            if (contains(minX, minY)) {
                containsCount++;
            }
            if (contains(maxX, minY)) {
                containsCount++;
            }
            if (containsCount == 1) {
                return containsCount;
            }
            if (contains(maxX, maxY)) {
                containsCount++;
            }
            if (containsCount == 2) {
                return containsCount;
            }
            if (contains(minX, maxY)) {
                containsCount++;
            }
            return containsCount;
        }

        private boolean crossesTriangle(
            double minX,
            double maxX,
            double minY,
            double maxY,
            double ax,
            double ay,
            double bx,
            double by,
            double cx,
            double cy,
            boolean includeBoundary
        ) {
            if (crossesDateline) {
                return crossesTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy, includeBoundary, lon -> lon < 0 ? lon + 360d : lon)
                    || crossesTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy, includeBoundary, lon -> lon > 0 ? lon - 360d : lon);
            } else {
                return crossesTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy, includeBoundary, DoubleUnaryOperator.identity());
            }
        }

        boolean crossesTriangle(
            double minX,
            double maxX,
            double minY,
            double maxY,
            double ax,
            double ay,
            double bx,
            double by,
            double cx,
            double cy,
            boolean includeBoundary,
            DoubleUnaryOperator normalizeLong
        ) {
            for (int i = 0; i < xs.length - 1; i++) {
                double dy = ys[i];
                double ey = ys[i + 1];
                double dx = normalizeLong.applyAsDouble(xs[i]);
                double ex = normalizeLong.applyAsDouble(xs[i + 1]);

                // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
                // if not, don't waste our time trying more complicated stuff
                boolean outside = (dy < minY && ey < minY)
                    || (dy > maxY && ey > maxY)
                    || (dx < minX && ex < minX)
                    || (dx > maxX && ex > maxX);

                if (outside == false) {
                    if (includeBoundary) {
                        if (lineCrossesLineWithBoundary(dx, dy, ex, ey, ax, ay, bx, by)
                            || lineCrossesLineWithBoundary(dx, dy, ex, ey, bx, by, cx, cy)
                            || lineCrossesLineWithBoundary(dx, dy, ex, ey, cx, cy, ax, ay)) {
                            return true;
                        }
                    } else {
                        if (lineCrossesLine(dx, dy, ex, ey, ax, ay, bx, by)
                            || lineCrossesLine(dx, dy, ex, ey, bx, by, cx, cy)
                            || lineCrossesLine(dx, dy, ex, ey, cx, cy, ax, ay)) {
                            return true;
                        }
                    }
                }

            }
            return false;
        }
    }
}
