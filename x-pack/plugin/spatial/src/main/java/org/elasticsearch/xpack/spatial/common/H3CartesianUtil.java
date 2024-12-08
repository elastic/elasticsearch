/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;

import java.util.HashMap;
import java.util.Map;
import java.util.function.DoubleUnaryOperator;

import static org.apache.lucene.geo.GeoUtils.lineCrossesLine;
import static org.apache.lucene.geo.GeoUtils.lineCrossesLineWithBoundary;

/**
 * Utility class that generates H3 bins coordinates projected on the cartesian plane (equirectangular projection).
 * Provides spatial methods to compute spatial intersections on those coordinates.
 */
public final class H3CartesianUtil {
    public static final int MAX_ARRAY_SIZE = 15;
    private static final DoubleUnaryOperator NORMALIZE_LONG_POS = lon -> lon < 0 ? lon + 360d : lon;
    private static final DoubleUnaryOperator NORMALIZE_LONG_NEG = lon -> lon > 0 ? lon - 360d : lon;

    // we cache the first two levels and polar polygons
    private static final Map<Long, double[][]> CACHED_H3 = new HashMap<>();
    static {
        for (long res0Cell : H3.getLongRes0Cells()) {
            CACHED_H3.put(res0Cell, getCoordinates(res0Cell));
            for (long h3 : H3.h3ToChildren(res0Cell)) {
                CACHED_H3.put(h3, getCoordinates(h3));
            }
        }
        for (int res = 2; res <= H3.MAX_H3_RES; res++) {
            CACHED_H3.put(H3.northPolarH3(res), getCoordinates(H3.northPolarH3(res)));
            CACHED_H3.put(H3.southPolarH3(res), getCoordinates(H3.southPolarH3(res)));
        }
    }

    private static final double[] NORTH_BOUND = new double[H3.MAX_H3_RES + 1];
    private static final double[] SOUTH_BOUND = new double[H3.MAX_H3_RES + 1];
    static {
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            NORTH_BOUND[res] = toBoundingBox(H3.northPolarH3(res)).getMinY();
            SOUTH_BOUND[res] = toBoundingBox(H3.southPolarH3(res)).getMaxY();
        }
    }

    /** For the given resolution, it returns true if the cell contains any of the poles */
    public static boolean isPolar(long h3) {
        final int res = H3.getResolution(h3);
        return H3.southPolarH3(res) == h3 || H3.northPolarH3(res) == h3;
    }

    /** For the given resolution, it returns the maximum latitude of the h3 bin containing the south pole */
    public static double getSouthPolarBound(int resolution) {
        return SOUTH_BOUND[resolution];
    }

    /** For the given resolution, it returns the minimum latitude of the h3 bin containing the north pole */
    public static double getNorthPolarBound(int resolution) {
        return NORTH_BOUND[resolution];
    }

    private static double[][] getCoordinates(final long h3) {
        final CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        final int numPoint = numPoints(h3, boundary);
        final double[] xs = new double[numPoint];
        final double[] ys = new double[numPoint];
        computePoints(h3, boundary, xs, ys);
        return new double[][] { xs, ys };
    }

    /** It stores the points for the given h3 in the provided arrays. The arrays
     * should be at least have the length of {@link #MAX_ARRAY_SIZE}. It returns the number of point added. */
    public static int computePoints(final long h3, final double[] xs, final double[] ys) {
        final double[][] cached = CACHED_H3.get(h3);
        if (cached != null) {
            System.arraycopy(cached[0], 0, xs, 0, cached[0].length);
            System.arraycopy(cached[1], 0, ys, 0, cached[0].length);
            return cached[0].length;
        }
        return computePoints(h3, H3.h3ToGeoBoundary(h3), xs, ys);
    }

    private static int numPoints(long h3, CellBoundary cellBoundary) {
        final int res = H3.getResolution(h3);
        if (h3 != H3.northPolarH3(res) && h3 != H3.southPolarH3(res)) {
            return cellBoundary.numPoints() + 1;
        } else {
            return cellBoundary.numPoints() + 5;
        }
    }

    private static int computePoints(final long h3, final CellBoundary cellBoundary, final double[] xs, final double[] ys) {
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            final LatLng latLng = cellBoundary.getLatLon(i);
            xs[i] = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(latLng.getLonDeg()));
            ys[i] = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(latLng.getLatDeg()));
        }
        final int numPoints = numPoints(h3, cellBoundary);
        final int res = H3.getResolution(h3);
        if (H3.northPolarH3(res) == h3) {
            closePolarComponent(xs, ys, cellBoundary.numPoints(), GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(90d)));
        } else if (H3.southPolarH3(res) == h3) {
            closePolarComponent(xs, ys, cellBoundary.numPoints(), GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(-90d)));
            // reverse points as we are building closing the points in reverse order
            ArrayUtils.reverseSubArray(xs, 0, numPoints);
            ArrayUtils.reverseSubArray(ys, 0, numPoints);
        } else {
            xs[cellBoundary.numPoints()] = xs[0];
            ys[cellBoundary.numPoints()] = ys[0];
        }
        return numPoints;
    }

    private static void closePolarComponent(double[] xs, double[] ys, int numBoundaryPoints, double pole) {
        sort(xs, ys, numBoundaryPoints);
        assert xs[0] > 0 != xs[numBoundaryPoints - 1] > 0 : "expected first and last element with different sign";
        final double y = datelineIntersectionLatitude(xs[0], ys[0], xs[numBoundaryPoints - 1], ys[numBoundaryPoints - 1]);
        xs[numBoundaryPoints] = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.MAX_LON_ENCODED);
        ys[numBoundaryPoints] = y;
        xs[numBoundaryPoints + 1] = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.MAX_LON_ENCODED);
        ys[numBoundaryPoints + 1] = pole;
        xs[numBoundaryPoints + 2] = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.MIN_LON_ENCODED);
        ys[numBoundaryPoints + 2] = pole;
        xs[numBoundaryPoints + 3] = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.MIN_LON_ENCODED);
        ys[numBoundaryPoints + 3] = y;
        xs[numBoundaryPoints + 4] = xs[0];
        ys[numBoundaryPoints + 4] = ys[0];
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

    private static double datelineIntersectionLatitude(double x1, double y1, double x2, double y2) {
        final double t = (180d - NORMALIZE_LONG_POS.applyAsDouble(x1)) / (NORMALIZE_LONG_POS.applyAsDouble(x2) - NORMALIZE_LONG_POS
            .applyAsDouble(x1));
        assert t > 0 && t <= 1;
        return y1 + t * (y2 - y1);
    }

    /** Return the {@link LatLonGeometry} representing the provided H3 bin */
    public static LatLonGeometry getLatLonGeometry(long h3) {
        return new H3CartesianGeometry(h3);
    }

    /** Return the {@link Geometry} representing the provided H3 bin */
    public static Geometry getNormalizeGeometry(long h3) {
        final double[][] cached = CACHED_H3.get(h3);
        final double[] xs;
        final double[] ys;
        if (cached != null) {
            xs = cached[0].clone();
            ys = cached[1].clone();
        } else {
            final CellBoundary boundary = H3.h3ToGeoBoundary(h3);
            final int numPoints = numPoints(h3, boundary);
            xs = new double[numPoints];
            ys = new double[numPoints];
            computePoints(h3, boundary, xs, ys);
        }
        final Polygon polygon = new Polygon(new LinearRing(xs, ys));
        if (isPolar(h3) || GeometryNormalizer.needsNormalize(Orientation.CCW, polygon) == false) {
            return polygon;
        }
        final Geometry geometry = GeometryNormalizer.apply(Orientation.CCW, polygon);
        if (geometry instanceof MultiPolygon) {
            return geometry;
        }
        // we shouldn't be here but one of the polygons crossing the dateline fails
        // to normalise, so we need to normalise it this way
        return GeometryNormalizer.apply(Orientation.CW, polygon);
    }

    /** Return the bounding box of the provided H3 bin */
    public static org.elasticsearch.geometry.Rectangle toBoundingBox(long h3) {
        final CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        double minLat = Double.POSITIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        double maxLon = Double.NEGATIVE_INFINITY;
        double maxNegLon = Double.NEGATIVE_INFINITY;
        double minPosLon = Double.POSITIVE_INFINITY;
        for (int i = 0; i < boundary.numPoints(); i++) {
            final double lon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(boundary.getLatLon(i).getLonDeg()));
            final double lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(boundary.getLatLon(i).getLatDeg()));
            minLat = Math.min(minLat, lat);
            minLon = Math.min(minLon, lon);
            maxLat = Math.max(maxLat, lat);
            maxLon = Math.max(maxLon, lon);
            if (lon < 0) {
                maxNegLon = Math.max(maxNegLon, lon);
            } else {
                minPosLon = Math.min(minPosLon, lon);
            }
        }
        final int res = H3.getResolution(h3);
        if (h3 == H3.northPolarH3(res)) {
            return new org.elasticsearch.geometry.Rectangle(-180d, 180d, 90d, minLat);
        } else if (h3 == H3.southPolarH3(res)) {
            return new org.elasticsearch.geometry.Rectangle(-180d, 180d, maxLat, -90d);
        } else if (maxLon - minLon > 180d) {
            return new org.elasticsearch.geometry.Rectangle(minPosLon, maxNegLon, maxLat, minLat);
        } else {
            return new org.elasticsearch.geometry.Rectangle(minLon, maxLon, maxLat, minLat);
        }
    }

    /** Return the spatial relationship between an H3 and a point.*/
    public static GeoRelation relatePoint(double[] xs, double[] ys, int numPoints, boolean crossesDateline, double x, double y) {
        final DoubleUnaryOperator normalizeLong = crossesDateline ? NORMALIZE_LONG_POS : DoubleUnaryOperator.identity();
        return relatePoint(xs, ys, numPoints, x, y, normalizeLong);
    }

    private static GeoRelation relatePoint(double[] xs, double[] ys, int numPoints, double x, double y, DoubleUnaryOperator normalize_lon) {
        boolean res = false;
        x = normalize_lon.applyAsDouble(x);
        for (int i = 0; i < numPoints - 1; i++) {
            final double x1 = normalize_lon.applyAsDouble(xs[i]);
            final double x2 = normalize_lon.applyAsDouble(xs[i + 1]);
            final double y1 = ys[i];
            final double y2 = ys[i + 1];
            if (y == y1 && y == y2 || (y <= y1 && y >= y2) != (y >= y1 && y <= y2)) {
                if ((x == x1 && x == x2) || ((x <= x1 && x >= x2) != (x >= x1 && x <= x2) && GeoUtils.orient(x1, y1, x2, y2, x, y) == 0)) {
                    return GeoRelation.QUERY_CROSSES;
                } else if (y1 > y != y2 > y) {
                    res ^= x < (x2 - x1) * (y - y1) / (y2 - y1) + x1;
                }
            }
        }
        return res ? GeoRelation.QUERY_CONTAINS : GeoRelation.QUERY_DISJOINT;
    }

    /** Checks if a line crosses a h3 bin.*/
    public static boolean crossesLine(
        double[] xs,
        double[] ys,
        int numPoints,
        boolean crossesDateline,
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
            return crossesLine(xs, ys, numPoints, minX, maxX, minY, maxY, aX, aY, bX, bY, includeBoundary, NORMALIZE_LONG_POS)
                || crossesLine(xs, ys, numPoints, minX, maxX, minY, maxY, aX, aY, bX, bY, includeBoundary, NORMALIZE_LONG_NEG);
        } else {
            return crossesLine(xs, ys, numPoints, minX, maxX, minY, maxY, aX, aY, bX, bY, includeBoundary, DoubleUnaryOperator.identity());
        }
    }

    private static boolean crossesLine(
        double[] xs,
        double[] ys,
        int numPoints,
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

        for (int i = 0; i < numPoints - 1; i++) {
            double cy = ys[i];
            double dy = ys[i + 1];
            double cx = normalizeLong.applyAsDouble(xs[i]);
            double dx = normalizeLong.applyAsDouble(xs[i + 1]);
            // compute bounding box of line
            double lMinX = StrictMath.min(cx, dx);
            double lMaxX = StrictMath.max(cx, dx);
            double lMinY = StrictMath.min(cy, dy);
            double lMaxY = StrictMath.max(cy, dy);

            // 2. check bounding boxes are disjoint
            if (lMaxX < minX || lMinX > maxX || lMinY > maxY || lMaxY < minY) {
                continue;
            }
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

    /** Checks if a triangle crosses a h3 bin.*/
    public static boolean crossesTriangle(
        double[] xs,
        double[] ys,
        int numPoints,
        boolean crossesDateline,
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
            return crossesTriangle(xs, ys, numPoints, minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy, includeBoundary, NORMALIZE_LONG_POS)
                || crossesTriangle(xs, ys, numPoints, minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy, includeBoundary, NORMALIZE_LONG_NEG);
        } else {
            return crossesTriangle(
                xs,
                ys,
                numPoints,
                minX,
                maxX,
                minY,
                maxY,
                ax,
                ay,
                bx,
                by,
                cx,
                cy,
                includeBoundary,
                DoubleUnaryOperator.identity()
            );
        }
    }

    private static boolean crossesTriangle(
        double[] xs,
        double[] ys,
        int numPoints,
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
        for (int i = 0; i < numPoints - 1; i++) {
            double dy = ys[i];
            double ey = ys[i + 1];
            double dx = normalizeLong.applyAsDouble(xs[i]);
            double ex = normalizeLong.applyAsDouble(xs[i + 1]);

            // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
            // if not, don't waste our time trying more complicated stuff
            boolean outside = (dy < minY && ey < minY) || (dy > maxY && ey > maxY) || (dx < minX && ex < minX) || (dx > maxX && ex > maxX);

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

    /** Checks if a rectangle crosses a h3 bin.*/
    public static boolean crossesBox(
        double[] xs,
        double[] ys,
        int numPoints,
        boolean crossesDateline,
        double minX,
        double maxX,
        double minY,
        double maxY,
        boolean includeBoundary
    ) {
        if (crossesDateline) {
            return crossesBox(xs, ys, numPoints, minX, maxX, minY, maxY, includeBoundary, NORMALIZE_LONG_POS)
                || crossesBox(xs, ys, numPoints, minX, maxX, minY, maxY, includeBoundary, NORMALIZE_LONG_NEG);
        } else {
            return crossesBox(xs, ys, numPoints, minX, maxX, minY, maxY, includeBoundary, DoubleUnaryOperator.identity());
        }
    }

    private static boolean crossesBox(
        double[] xs,
        double[] ys,
        int numPoints,
        double minX,
        double maxX,
        double minY,
        double maxY,
        boolean includeBoundary,
        DoubleUnaryOperator normalizeLong
    ) {
        // we just have to cross one edge to answer the question, so we descend the tree and return when
        // we do.
        for (int i = 0; i < numPoints - 1; i++) {
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
            boolean outside = (cy < minY && dy < minY) || (cy > maxY && dy > maxY) || (cx < minX && dx < minX) || (cx > maxX && dx > maxX);

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
}
