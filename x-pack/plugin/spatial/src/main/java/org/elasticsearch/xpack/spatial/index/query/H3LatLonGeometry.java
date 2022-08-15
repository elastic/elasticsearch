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

        private final long h3;
        private final int res;
        private final GeoPolygon hexagon;

        private H3Polygon2D(String h3Address) {
            h3 = H3.stringToH3(h3Address);
            res = H3.getResolution(h3Address);
            hexagon = getGeoPolygon(h3Address);
            // I tried to compute the bounding box to set min/max values, but it seems to fail
            // due to numerical errors. For now, we just don't use it, this means we will not be
            // using the optimization provided by lucene's ComponentPredicate.

        }

        private GeoPolygon getGeoPolygon(String h3Address) {
            final CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3Address);
            final List<GeoPoint> points = new ArrayList<>(cellBoundary.numPoints());
            for (int i = 0; i < cellBoundary.numPoints(); i++) {
                final LatLng latLng = cellBoundary.getLatLon(i);
                points.add(new GeoPoint(PlanetModel.SPHERE, latLng.getLatRad(), latLng.getLonRad()));
            }
            return GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
        }

        @Override
        public double getMinX() {
            return GeoUtils.MIN_LON_INCL;
        }

        @Override
        public double getMaxX() {
            return GeoUtils.MAX_LON_INCL;
        }

        @Override
        public double getMinY() {
            return GeoUtils.MIN_LAT_INCL;
        }

        @Override
        public double getMaxY() {
            return GeoUtils.MAX_LAT_INCL;
        }

        @Override
        public boolean contains(double x, double y) {
            return h3 == H3.geoToH3(y, x, res);
        }

        @Override
        public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
            GeoArea box = GeoAreaFactory.makeGeoArea(
                PlanetModel.SPHERE,
                Math.toRadians(maxY),
                Math.toRadians(minY),
                Math.toRadians(minX),
                Math.toRadians(maxX)
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
