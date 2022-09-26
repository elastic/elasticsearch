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
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.LatLonBounds;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Implementation of a lucene {@link LatLonGeometry} that covers the extent of a provided H3 bin. Note that
 * H3 bin are polygons on the sphere. */
class H3LatLonGeometry extends LatLonGeometry {

    private final String h3Address;

    H3LatLonGeometry(String h3Address) {
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
        StringBuilder sb = new StringBuilder();
        sb.append("H3 : ");
        sb.append("\"");
        sb.append(h3Address);
        sb.append("\"");
        return sb.toString();
    }

    private static class H3Polygon2D implements Component2D {

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

        private GeoPolygon getGeoPolygon(CellBoundary cellBoundary) {
            final List<GeoPoint> points = new ArrayList<>(cellBoundary.numPoints());
            for (int i = 0; i < cellBoundary.numPoints(); i++) {
                final LatLng latLng = cellBoundary.getLatLon(i);
                points.add(new GeoPoint(PlanetModel.SPHERE, latLng.getLatRad(), latLng.getLonRad()));
            }
            return GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
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
            if (minX > this.maxX || maxX < this.minX || maxY < this.minY || minY > this.maxY) {
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
            throw new UnsupportedOperationException("intersectsLine not implemented in H3Polygon2D");
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
            throw new UnsupportedOperationException("intersectsTriangle not implemented in H3Polygon2D");
        }

        @Override
        public boolean containsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
            throw new UnsupportedOperationException("containsLine not implemented in H3Polygon2D");
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
            throw new IllegalArgumentException();
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
            throw new UnsupportedOperationException("withinLine not implemented in H3Polygon2D");
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
            throw new UnsupportedOperationException("withinTriangle not implemented in H3Polygon2D");
        }
    }
}
