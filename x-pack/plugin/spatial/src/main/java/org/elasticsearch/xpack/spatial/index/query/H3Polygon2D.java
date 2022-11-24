/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.spatial3d.geom.GeoArea;
import org.apache.lucene.spatial3d.geom.GeoAreaFactory;
import org.apache.lucene.spatial3d.geom.GeoAreaShape;
import org.apache.lucene.spatial3d.geom.GeoPath;
import org.apache.lucene.spatial3d.geom.GeoPathFactory;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.LatLonBounds;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.spatial.geom.GeoRegularConvexPolygon;
import org.elasticsearch.xpack.spatial.geom.GeoRegularConvexPolygonFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.PI;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toDegrees;
import static java.lang.Math.toRadians;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.calculateCentroid;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.calculateCentroid3d;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.pointInterpolation;
import static org.elasticsearch.xpack.spatial.index.query.NormalizedH3Polygon2D.NEITHER;
import static org.elasticsearch.xpack.spatial.index.query.NormalizedH3Polygon2D.NORTH;
import static org.elasticsearch.xpack.spatial.index.query.NormalizedH3Polygon2D.SOUTH;

/**
 * Implementation of a lucene {@link LatLonGeometry} that covers the extent of a provided H3 bin.
 *
 * Since H3 bins are polygons on the sphere, we internally represent them as polygons using the Lucene spatial3d package.
 * As such this class represents an interface between the Lucene 2D world of Component2D and the Lucene spatial3d world
 * which performs all the mathematics on the sphere (using 3d x, y, z coordinates). In particular the toComponent2D method
 * will return an object with all methods implemented using spatial3d functions, which are geometrically accurate in that they
 * use great circles to model straight lines between points.
 *
 * This makes it possible to compare H3 cells to existing indexed (and triangulated) lucene geometries, like Polygon2D for example.
 * To achieve this, we make use of a model of a hexagon that implements the org.apache.lucene.spatial3d.geom.GeoPolygon interface.
 * The methods implemented for the Component2D interface are essentially copies of the same methods in Polygon2D,
 * but with the internal logic modified in two specific directions:
 * <ul>
 *     <li>Intersections between lines make use of great circles</li>
 *     <li>The fact that this object is a simple convex polygon with no holes allows for some optimizations</li>
 * </ul>
 *
 * Note that H3 cells are simple convex polygons except where they intersect the edges of the original icosohedron triangles from
 * which the H3 model is derived. Those edge cells are more complex to work with. As such we divide the H3 cells into two groups:
 * <ol>
 *     <li>Plain hexagons that are convex polygons, and we use a simple optimized implementation of GeoRegularConvexPolygon</li>
 *     <li>Other cells revert to the spatial3d class GeoPolygon created using GeoPolygonFactory</li>
 * </ol>
 *
 * In addition, two implementations are provided:
 * <ul>
 *     <li>H3LatLonGeometry - this represents an H3 cell as a GeoPolygon matching the cell boundary</li>
 *     <li>H3LatLonGeometry.Scaled - this mimics an H3 cell scaled up in size, linearly from the centroid</li>
 * </ul>
 * The scaled version is useful for performing geometry intersection tests that cover all the child cells, since in H3
 * child cells extend outside the bounds of the parent cells. If we want to know if any of an H3 cells children might
 * intersect, we can test a scaled version of the parent first, before iterating through the child cells.
 */

/**
 * This class implements the Component2D interface in order to be useful in comparing hexagons to existing indexed (and triangulated)
 * lucene geometries, like Polygon2D for example. However, all the internal mathematics is based instead on the geometrically
 * accurate code in org.apache.lucene.spatial3d package which uses great circles to model straight lines between points.
 * <p>
 * Two implementations are provided:
 * <ul>
 *     <li>Unscaled - this represents an H3 cell as a GeoPolygon matching the cell boundary</li>
 *     <li>Scaled - this mimics an H3 cell scaled up in size, linearly from the centroid</li>
 * </ul>
 */
abstract class H3Polygon2D implements Component2D {
    // We want to make are edges a bit bigger because spatial3d and h3 edges do not fully agree in
    // membership of points around he edges.
    private static final double BBOX_EDGE_DELTA = 1e-4;
    protected final long h3;
    protected final int res;
    private double minX, maxX, minY, maxY;

    private H3Polygon2D(String h3Address) {
        h3 = H3.stringToH3(h3Address);
        res = H3.getResolution(h3Address);
    }

    protected void setBounds(double minX, double maxX, double minY, double maxY) {
        // Unfortunately, h3 bin edges are fuzzy and cannot be represented easily. We need to buffer
        // the bounding boxes to make sure we don't reject valid points
        this.minX = max(GeoUtils.MIN_LON_INCL, minX - BBOX_EDGE_DELTA);
        this.maxX = min(GeoUtils.MAX_LON_INCL, maxX + BBOX_EDGE_DELTA);
        this.minY = max(GeoUtils.MIN_LAT_INCL, minY - BBOX_EDGE_DELTA);
        this.maxY = min(GeoUtils.MAX_LAT_INCL, maxY + BBOX_EDGE_DELTA);
    }

    protected boolean disjointBounds(double minX, double maxX, double minY, double maxY) {
        return minX > this.maxX || maxX < this.minX || maxY < this.minY || minY > this.maxY;
    }

    @Override
    public double getMinX() {
        return minX;
    }

    @Override
    public double getMaxX() {
        return maxX;
    }

    @Override
    public double getMinY() {
        return minY;
    }

    @Override
    public double getMaxY() {
        return maxY;
    }

    /**
     * Interface allowing external code to perform inspections of this class.
     * Just call the H3Polygon2D.inspect method with an instance of this interface to be informed.
     * Currently, this is used primary for testing purposes.
     */
    interface CellInspector {
        void inspect(long h3, int res, double minX, double maxX, double minY, double maxY, List<Point> boundary);
    }

    abstract GeoPoint[] boundaryPoints();

    void inspect(CellInspector inspect) {
        ArrayList<Point> boundary = new ArrayList<>();
        for (GeoPoint vertex : boundaryPoints()) {
            boundary.add(new Point(toDegrees(vertex.getLongitude()), toDegrees(vertex.getLatitude())));
        }
        boundary.add(boundary.get(0));
        inspect.inspect(h3, res, minX, maxX, minY, maxY, boundary);
    }

    abstract static class Planar extends H3Polygon2D {
        protected Component2D hexagon;
        private static final long northPole0 = H3.geoToH3(90, 0, 0);
        private static final long southPole0 = H3.geoToH3(-90, 0, 0);
        private static final long northPole1 = H3.geoToH3(90, 0, 1);
        private static final long southPole1 = H3.geoToH3(-90, 0, 1);

        static class Unscaled extends Planar {
            Unscaled(String h3Address) {
                super(h3Address);
                this.hexagon = initialize(unscaledBoundary(H3.h3ToGeoBoundary(h3Address)), pole(h3Address));
            }

            GeoPoint[] boundaryPoints() {
                // We need to mimic the same behaviour of the initialize method, to capture the inner normalized polygon for testing
                GeoPoint[] boundary = unscaledBoundary(H3.h3ToGeoBoundary(h3));
                BoundaryListener listener = new BoundaryListener();
                initialize(boundary, pole(H3.h3ToString(h3)), listener);
                return listener.getCapturedBoundary(boundary);
            }
        }

        static class Scaled extends Planar {
            private final double scaleFactor;

            Scaled(String h3Address, double scaleFactor) {
                super(h3Address);
                this.scaleFactor = scaleFactor;
                GeoPoint[] boundary = scaledBoundary(H3.h3ToGeoBoundary(h3Address), scaleFactor);
                int pole = pole(h3Address);
                if (pole == NEITHER) {
                    pole = checkPole(boundary);
                }
                try {
                    this.hexagon = initialize(boundary, pole);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to normalize cell " + h3Address + " scaled to " + scaleFactor, e);
                }
            }

            /**
             * When scaling an H3 cell, it could happen that a cell that previously did not contain a pole, now contains a pole.
             */
            private int checkPole(GeoPoint[] boundary) {
                GeoPoint centroid = calculateCentroid(boundary);
                for (int pole : new int[] { 1, -1 }) {
                    if (checkPole(centroid, boundary, pole)) {
                        return pole;
                    }
                }
                return NEITHER;
            }

            private boolean checkPole(GeoPoint centroid, GeoPoint[] boundary, int pole) {
                // TODO: figure out if this is the correct threshold
                double angleToPole = centroid.arcDistance(0, 0, pole);
                if (angleToPole < PI / 8) {
                    // If we are close to the pole, do an expensive check
                    List<GeoPoint> points = Arrays.asList(boundary);
                    GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
                    return polygon.isWithin(0, 0, pole);
                }
                return false;
            }

            GeoPoint[] boundaryPoints() {
                // We need to mimic the same behaviour of the initialize method, to capture the inner normalized polygon for testing
                GeoPoint[] boundary = scaledBoundary(H3.h3ToGeoBoundary(h3), scaleFactor);
                int pole = pole(H3.h3ToString(h3));
                if (pole == NEITHER) {
                    pole = checkPole(boundary);
                }
                BoundaryListener listener = new BoundaryListener();
                initialize(boundary, pole, listener);
                return listener.getCapturedBoundary(boundary);
            }
        }

        private static class BoundaryListener implements NormalizedH3Polygon2D.Listener {
            private GeoPoint[] boundary;

            @Override
            public void polygon(double[] lats, double[] lons, double offset) {
                boundary = new GeoPoint[lons.length];
                for (int i = 0; i < lons.length; i++) {
                    double lon = NormalizedH3Polygon2D.n(lons[i] - offset);
                    boundary[i] = new GeoPoint(PlanetModel.SPHERE, toRadians(lats[i]), toRadians(lon));
                }
            }

            GeoPoint[] getCapturedBoundary(GeoPoint[] original) {
                return boundary != null ? boundary : original;
            }
        }

        private static int pole(final String h3Address) {
            long h3 = H3.stringToH3(h3Address);
            if (h3 == northPole0 || h3 == northPole1) {
                return NORTH;
            } else if (h3 == southPole0 || h3 == southPole1) {
                return SOUTH;
            } else {
                return NEITHER;
            }
        }

        private Planar(String h3Address) {
            super(h3Address);
        }

        protected Component2D initialize(final GeoPoint[] cellBoundary, final int pole) {
            return initialize(cellBoundary, pole, null);
        }

        protected Component2D initialize(final GeoPoint[] cellBoundary, final int pole, NormalizedH3Polygon2D.Listener listener) {
            double[] lats = new double[cellBoundary.length + 1];
            double[] lons = new double[cellBoundary.length + 1];
            double minX = Float.MAX_VALUE;
            double maxX = -Float.MAX_VALUE;
            double minY = Float.MAX_VALUE;
            double maxY = -Float.MAX_VALUE;
            for (int i = 0; i < cellBoundary.length; i++) {
                lats[i] = toDegrees(cellBoundary[i].getLatitude());
                lons[i] = normalizeLongitude(toDegrees(cellBoundary[i].getLongitude()));
                minX = min(minX, lons[i]);
                maxX = max(maxX, lons[i]);
                minY = min(minY, lats[i]);
                maxY = max(maxY, lats[i]);
            }
            lats[cellBoundary.length] = lats[0];
            lons[cellBoundary.length] = lons[0];
            if (maxX - minX > 180 || maxY > GeoTileUtils.LATITUDE_MASK || minY < -GeoTileUtils.LATITUDE_MASK) {
                // We crossed the dateline, or latitude limits, normalize the polygon
                Component2D normalized;
                try {
                    normalized = new NormalizedH3Polygon2D(lats, lons, pole, GeoTileUtils.LATITUDE_MASK, listener);
                } catch (IllegalArgumentException e) {
                    // Polygon is entirely north of 85 degrees or south of -85 degrees
                    normalized = new EmptyComponent2D();
                }
                minX = normalized.getMinX();
                maxX = normalized.getMaxX();
                minY = normalized.getMinY();
                maxY = normalized.getMaxY();
                if (minX > maxX) {
                    // If we cross the dateline, then disregard min/max X bounds in bounds check, and let the inner component handle that
                    minX = GeoUtils.MIN_LON_INCL;
                    maxX = GeoUtils.MAX_LON_INCL;
                }
                setBounds(minX, maxX, minY, maxY);
                return normalized;
            } else {
                // We did not cross the dateline, use a standard Lucene polygon
                setBounds(minX, maxX, minY, maxY);
                return LatLonGeometry.create(new Polygon(lats, lons));
            }
        }

        private static double normalizeLongitude(double value) {
            while (value < -180) {
                value += 360;
            }
            while (value > 180) {
                value -= 360;
            }
            return value;
        }

        @Override
        public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
            return hexagon.relate(minX, maxX, minY, maxY);
        }

        /**
         * Note: we are not using the H3.geoToH3 method,
         * so this could return different results, especially for large cells near the poles
         */
        @Override
        public boolean contains(double x, double y) {
            if (disjointBounds(x, x, y, y)) {
                return false;
            }
            return hexagon.contains(x, y);
        }

        @Override
        public boolean intersectsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return false;
            }
            return hexagon.intersectsLine(minX, maxX, minY, maxY, aX, aY, bX, bY);
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
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return false;
            }
            return hexagon.intersectsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
        }

        @Override
        public boolean containsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return false;
            }
            return hexagon.containsLine(minX, maxX, minY, maxY, aX, aY, bX, bY);
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
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return false;
            }
            return hexagon.containsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
        }

        @Override
        public WithinRelation withinPoint(double x, double y) {
            return hexagon.withinPoint(x, y);
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
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return WithinRelation.DISJOINT;
            }
            return hexagon.withinLine(minX, maxX, minY, maxY, aX, aY, ab, bX, bY);
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
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return WithinRelation.DISJOINT;
            }
            return hexagon.withinTriangle(minX, maxX, minY, maxY, aX, aY, ab, bX, bY, bc, cX, cY, ca);
        }
    }

    abstract static class Spherical extends H3Polygon2D {
        protected final GeoPolygon hexagon;

        static class Unscaled extends Spherical {
            Unscaled(String h3Address) {
                super(h3Address, unscaledBoundary(H3.h3ToGeoBoundary(h3Address)));
            }

            @Override
            GeoPoint[] boundaryPoints() {
                return unscaledBoundary(H3.h3ToGeoBoundary(h3));
            }

            @Override
            public boolean contains(double x, double y) {
                return h3 == H3.geoToH3(y, x, res);
            }

        }

        static class Scaled extends Spherical {
            private final double scaleFactor;

            Scaled(String h3Address, double scaleFactor) {
                super(h3Address, scaledBoundary(H3.h3ToGeoBoundary(h3Address), scaleFactor));
                this.scaleFactor = scaleFactor;
            }

            @Override
            GeoPoint[] boundaryPoints() {
                return scaledBoundary(H3.h3ToGeoBoundary(h3), scaleFactor);
            }

            @Override
            public boolean contains(double x, double y) {
                if (disjointBounds(x, x, y, y)) {
                    return false;
                }
                double lat = Math.toRadians(y);
                double lon = Math.toRadians(x);
                final GeoArea box = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, lat, lat, lon, lon);
                return box.getRelationship(hexagon) != GeoArea.DISJOINT;
            }
        }

        private Spherical(String h3Address, final GeoPoint[] cellBoundary) {
            super(h3Address);
            hexagon = getGeoPolygon(cellBoundary);
            final LatLonBounds bounds = new LatLonBounds();
            hexagon.getBounds(bounds);
            final double minY = bounds.checkNoBottomLatitudeBound() ? GeoUtils.MIN_LAT_INCL : toDegrees(bounds.getMinLatitude());
            final double maxY = bounds.checkNoTopLatitudeBound() ? GeoUtils.MAX_LAT_INCL : toDegrees(bounds.getMaxLatitude());
            final double minX;
            final double maxX;
            if (bounds.checkNoLongitudeBound() || bounds.getLeftLongitude() > bounds.getRightLongitude()) {
                minX = GeoUtils.MIN_LON_INCL;
                maxX = GeoUtils.MAX_LON_INCL;
            } else {
                minX = toDegrees(bounds.getLeftLongitude());
                maxX = toDegrees(bounds.getRightLongitude());
            }
            setBounds(minX, maxX, minY, maxY);
        }

        private GeoPolygon getGeoPolygon(final GeoPoint[] points) {
            return points.length > 6
                ? GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, Arrays.stream(points).toList())
                : GeoRegularConvexPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
        }

        @Override
        public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
            // h3 edges are fuzzy, therefore to avoid issues when bounding box are around the edges,
            // we just buffer slightly the bounding box to check if it is inside the h3 bin, otherwise
            // return crosses.
            final GeoArea box = GeoAreaFactory.makeGeoArea(
                PlanetModel.SPHERE,
                Math.toRadians(min(GeoUtils.MAX_LAT_INCL, maxY + BBOX_EDGE_DELTA)),
                Math.toRadians(max(GeoUtils.MIN_LAT_INCL, minY - BBOX_EDGE_DELTA)),
                Math.toRadians(max(GeoUtils.MIN_LON_INCL, minX - BBOX_EDGE_DELTA)),
                Math.toRadians(min(GeoUtils.MAX_LON_INCL, maxX + BBOX_EDGE_DELTA))
            );
            return switch (box.getRelationship(hexagon)) {
                case GeoArea.CONTAINS -> PointValues.Relation.CELL_INSIDE_QUERY;
                case GeoArea.DISJOINT -> PointValues.Relation.CELL_OUTSIDE_QUERY;
                default -> PointValues.Relation.CELL_CROSSES_QUERY;
            };
        }

        @Override
        public boolean intersectsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return false;
            }
            return crossesLine(aX, aY, bX, bY);
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
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return false;
            }
            if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
                // If any corner of the triangle is within the hexagon, we intersect the triangle since we have no holes
                return true;
            }
            // But if all corners are outside, we still need to do a more comprehensive search in case the hexagon is within the
            // triangle
            return anyPointInside(makeTriangle(aX, aY, bX, bY, cX, cY));
        }

        /**
         * Determine if any hexagon point is within the triangle. If the hexagon is not a GeoRegularConvexPolygon
         * fall-back on determining the relationship between the two GeoPolygon objects.
         */
        private boolean anyPointInside(GeoAreaShape triangle) {
            if (hexagon instanceof GeoRegularConvexPolygon polygon) {
                // Perform a faster check if we have a regular convex polygon
                return polygon.anyPointInside(triangle);
            } else {
                return hexagon.getRelationship(triangle) != GeoArea.DISJOINT;
            }
        }

        @Override
        public boolean containsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return false;
            }
            // If both ends of the line are within this hexagon, then the entire line is within the hexagon since we have no holes
            return contains(aX, aY) && contains(bX, bY);
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
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return false;
            }
            // If all corners of the triangle are within this hexagon, then the entire triangle is within the hexagon since we have no
            // holes
            return contains(aX, aY) && contains(bX, bY) && contains(cX, cY);
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
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return WithinRelation.DISJOINT;
            }
            // If either end is within the hexagon, then the hexagon is not within the shape for which the line is a component
            if (contains(aX, aY) || contains(bX, bY)) {
                return WithinRelation.NOTWITHIN;
            }
            // If the line is part of the outside of the shape, and it crosses the hexagon, then the hexagon is not within the shape
            if (ab && crossesLine(aX, aY, bX, bY)) {
                return WithinRelation.NOTWITHIN;
            }
            // If the line is not part of the original shape (ie. inner triangle line), it does not matter if it crosses the hexagon or
            // not
            // TODO: Verify why we never return CANDIDATE (this code is copied from Polygon2D which returns DISJOINT at this point)
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
            if (disjointBounds(minX, maxX, minY, maxY)) {
                return WithinRelation.DISJOINT;
            }

            // if any of the triangle vertices is inside the hexagon, the hexagon cannot be within this indexed
            // shape because all triangle vertices belong to the original indexed shape.
            if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
                return WithinRelation.NOTWITHIN;
            }

            WithinRelation relation = WithinRelation.DISJOINT;
            // if any of the triangle edges intersects an edge belonging to the shape then it cannot be within.
            // if it only intersects edges that do not belong to the shape, then it is a candidate.
            // we skip edges at the dateline to support shapes crossing it
            if (crossesLine(aX, aY, bX, bY)) {
                if (ab) {
                    return WithinRelation.NOTWITHIN;
                } else {
                    relation = WithinRelation.CANDIDATE;
                }
            }

            if (crossesLine(bX, bY, cX, cY)) {
                if (bc) {
                    return WithinRelation.NOTWITHIN;
                } else {
                    relation = WithinRelation.CANDIDATE;
                }
            }

            if (crossesLine(cX, cY, aX, aY)) {
                if (ca) {
                    return WithinRelation.NOTWITHIN;
                } else {
                    relation = WithinRelation.CANDIDATE;
                }
            }

            // if any of the hexagon edges crosses a triangle edge that does not belong to the original then it is a candidate for
            // within
            if (relation == WithinRelation.CANDIDATE) {
                return WithinRelation.CANDIDATE;
            }

            // We can only get to this stage if the entire triangle is outside the hexagon, or the entire hexagon
            // is contained within the triangle, but no points or edges intersect. It is sufficient to test
            // that a single point of the hexagon is within the triangle to return this triangle as a candidate.
            return anyPointInside(makeTriangle(aX, aY, bX, bY, cX, cY)) ? WithinRelation.CANDIDATE : WithinRelation.DISJOINT;
        }

        private GeoAreaShape makeTriangle(double aX, double aY, double bX, double bY, double cX, double cY) {
            return GeoRegularConvexPolygonFactory.makeGeoPolygon(
                PlanetModel.SPHERE,
                new GeoPoint(PlanetModel.SPHERE, Math.toRadians(aY), Math.toRadians(aX)),
                new GeoPoint(PlanetModel.SPHERE, Math.toRadians(bY), Math.toRadians(bX)),
                new GeoPoint(PlanetModel.SPHERE, Math.toRadians(cY), Math.toRadians(cX))
            );
        }

        /**
         * Returns true if the line crosses any edge of this hexagon
         */
        private boolean crossesLine(double aX, double aY, double bX, double bY) {
            // TODO: currently just relying on GeoPathFactory.makeGeoPath to make a usable GeoShape for intersects.
            // But this could be expensive. Since we only ever use this for testing line intersection with the hexagon,
            // there could be a simpler structure to do just that specific test.
            // Look into org.apache.lucene.spatial3d code for line intersection code for this.
            final GeoPoint[] points = new GeoPoint[2];
            points[0] = new GeoPoint(PlanetModel.SPHERE, Math.toRadians(aY), Math.toRadians(aX));
            points[1] = new GeoPoint(PlanetModel.SPHERE, Math.toRadians(bY), Math.toRadians(bX));
            GeoPath line = GeoPathFactory.makeGeoPath(PlanetModel.SPHERE, 0, points);

            return hexagon.intersects(line);
        }
    }

    private static class EmptyComponent2D implements Component2D {

        @Override
        public double getMinX() {
            return GeoUtils.MAX_LON_INCL;
        }

        @Override
        public double getMaxX() {
            return GeoUtils.MIN_LON_INCL;
        }

        @Override
        public double getMinY() {
            return GeoUtils.MAX_LAT_INCL;
        }

        @Override
        public double getMaxY() {
            return GeoUtils.MIN_LAT_INCL;
        }

        @Override
        public boolean contains(double x, double y) {
            return false;
        }

        @Override
        public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
            return null;
        }

        @Override
        public boolean intersectsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            return false;
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
            return false;
        }

        @Override
        public boolean containsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            return false;
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
            return false;
        }

        @Override
        public WithinRelation withinPoint(double x, double y) {
            return null;
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
            return null;
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
            return null;
        }
    }

    static GeoPoint[] scaledBoundary(CellBoundary cellBoundary, double scaleFactor) {
        GeoPoint centroid = calculateCentroid3d(cellBoundary);
        final GeoPoint[] points = new GeoPoint[cellBoundary.numPoints()];
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            LatLng point = cellBoundary.getLatLon(i);
            GeoPoint geoPoint = new GeoPoint(PlanetModel.SPHERE, point.getLatRad(), point.getLonRad());
            points[i] = pointInterpolation(centroid, geoPoint, scaleFactor);
        }
        return points;
    }

    static GeoPoint[] unscaledBoundary(CellBoundary cellBoundary) {
        final GeoPoint[] points = new GeoPoint[cellBoundary.numPoints()];
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            final LatLng latLng = cellBoundary.getLatLon(i);
            points[i] = new GeoPoint(PlanetModel.SPHERE, latLng.getLatRad(), latLng.getLonRad());
        }
        return points;
    }
}
