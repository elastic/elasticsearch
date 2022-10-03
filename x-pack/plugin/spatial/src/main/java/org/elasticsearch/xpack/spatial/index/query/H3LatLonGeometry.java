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
import org.elasticsearch.xpack.spatial.geom.GeoRegularConvexPolygon;
import org.elasticsearch.xpack.spatial.geom.GeoRegularConvexPolygonFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Implementation of a lucene {@link LatLonGeometry} that covers the extent of a provided H3 bin. Note that
 * H3 bin are polygons on the sphere. */
public class H3LatLonGeometry extends LatLonGeometry {

    private final String h3Address;

    public H3LatLonGeometry(String h3Address) {
        this.h3Address = h3Address;
    }

    @Override
    protected Component2D toComponent2D() {
        return new H3Polygon2D(h3Address);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof H3LatLonGeometry h3) {
            return Objects.equals(h3Address, h3.h3Address);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(h3Address);
    }

    @Override
    public String toString() {
        return "H3 : " + "\"" + h3Address + "\"";
    }

    /**
     * This class implements the Component2D interface in order to be useful in comparing hexagons to existing indexed (and triangulated)
     * lucene geometries, like Polygon2D for example. However, all the internal mathematics is based instead on the geometrically
     * accurate code in org.apache.lucene.spatial3d package which uses great circles to model straight lines between points.
     * To achieve this, we make use of a model of a hexagon that implements the org.apache.lucene.spatial3d.geom.GeoPolygon
     * interface. The methods implemented here are essentially copies of the same methods in Polygon2D, but with the internal logic
     * modified in two specific directions:
     * <ul>
     *     <li>Intersections between lines make use of great circles</li>
     *     <li>The fact that this object is a simple convex polygon with no holes allows for some optimizations</li>
     * </ul>
     * TODO: Move this comment to the public class comment above for more visibility
     */
    static class H3Polygon2D implements Component2D {

        // We want to make are edges a bit bigger because spatial3d and h3 edges do not fully agree in
        // membership of points around he edges.
        private static final double BBOX_EDGE_DELTA = 1e-4;
        private final long h3;
        private final int res;
        private final GeoPolygon hexagon;
        private final double minX, maxX, minY, maxY;

        private H3Polygon2D(String h3Address) {
            h3 = H3.stringToH3(h3Address);
            res = H3.getResolution(h3Address);
            final CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3Address);
            hexagon = getGeoPolygon(cellBoundary);
            final LatLonBounds bounds = new LatLonBounds();
            hexagon.getBounds(bounds);
            final double minY = bounds.checkNoBottomLatitudeBound() ? GeoUtils.MIN_LAT_INCL : Math.toDegrees(bounds.getMinLatitude());
            final double maxY = bounds.checkNoTopLatitudeBound() ? GeoUtils.MAX_LAT_INCL : Math.toDegrees(bounds.getMaxLatitude());
            final double minX;
            final double maxX;
            if (bounds.checkNoLongitudeBound() || bounds.getLeftLongitude() > bounds.getRightLongitude()) {
                minX = GeoUtils.MIN_LON_INCL;
                maxX = GeoUtils.MAX_LON_INCL;
            } else {
                minX = Math.toDegrees(bounds.getLeftLongitude());
                maxX = Math.toDegrees(bounds.getRightLongitude());
            }
            // Unfortunately, h3 bin edges are fuzzy and cannot be represented easily. We need to buffer
            // the bounding boxes to make sure we don't reject valid points
            this.minX = Math.max(GeoUtils.MIN_LON_INCL, minX - BBOX_EDGE_DELTA);
            this.maxX = Math.min(GeoUtils.MAX_LON_INCL, maxX + BBOX_EDGE_DELTA);
            this.minY = Math.max(GeoUtils.MIN_LAT_INCL, minY - BBOX_EDGE_DELTA);
            this.maxY = Math.min(GeoUtils.MAX_LAT_INCL, maxY + BBOX_EDGE_DELTA);
        }

        /**
         * Interface allowing external code to perform inspections of this class.
         * Just call the H3Polygon2D.inspect method with an instance of this interface to be informed.
         * Currently, this is used primary for testing purposes.
         */
        interface CellInspector {
            void inspect(long h3, int res, double minX, double maxX, double minY, double maxY, List<Point> boundary);
        }

        void inspect(CellInspector inspect) {
            final CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3);
            ArrayList<Point> boundary = new ArrayList<>(cellBoundary.numPoints());
            for (int i = 0; i < cellBoundary.numPoints(); i++) {
                final LatLng latLng = cellBoundary.getLatLon(i);
                boundary.add(new Point(latLng.getLonDeg(), latLng.getLatDeg()));
            }
            inspect.inspect(h3, res, minX, maxX, minY, maxY, boundary);
        }

        private GeoPolygon getGeoPolygon(CellBoundary cellBoundary) {
            final GeoPoint[] points = new GeoPoint[cellBoundary.numPoints()];
            for (int i = 0; i < cellBoundary.numPoints(); i++) {
                final LatLng latLng = cellBoundary.getLatLon(i);
                points[i] = new GeoPoint(PlanetModel.SPHERE, latLng.getLatRad(), latLng.getLonRad());
            }
            return points.length > 6
                ? GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, Arrays.stream(points).toList())
                : GeoRegularConvexPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
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

        @Override
        public boolean contains(double x, double y) {
            return h3 == H3.geoToH3(y, x, res);
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
                Math.toRadians(Math.min(GeoUtils.MAX_LAT_INCL, maxY + BBOX_EDGE_DELTA)),
                Math.toRadians(Math.max(GeoUtils.MIN_LAT_INCL, minY - BBOX_EDGE_DELTA)),
                Math.toRadians(Math.max(GeoUtils.MIN_LON_INCL, minX - BBOX_EDGE_DELTA)),
                Math.toRadians(Math.min(GeoUtils.MAX_LON_INCL, maxX + BBOX_EDGE_DELTA))
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
            // But if all corners are outside, we still need to do a more comprehensive search in case the hexagon is within the triangle
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
            // If all corners of the triangle are within this hexagon, then the entire triangle is within the hexagon since we have no holes
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
            // If the line is not part of the original shape (ie. inner triangle line), it does not matter if it crosses the hexagon or not
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

            // if any of the hexagon edges crosses a triangle edge that does not belong to the original then it is a candidate for within
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

        private boolean disjointBounds(double minX, double maxX, double minY, double maxY) {
            return minX > this.maxX || maxX < this.minX || maxY < this.minY || minY > this.maxY;
        }

        /** Returns true if the line crosses any edge of this hexagon */
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
}
