/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * Utility class for generating H3 spherical objects.
 * TODO: This class is a copy of the same class in org.elasticsearch.xpack.spatial.common, we should find a common location for it.
 */
public final class H3SphericalUtil {

    private static final BiPredicate<LatLng, LatLng> MIN_COMPARATOR = (e1, e2) -> e1.getLatRad() < e2.getLatRad();

    private static final BiPredicate<LatLng, LatLng> MAX_COMPARATOR = (e1, e2) -> e1.getLatRad() > e2.getLatRad();

    /**
     * Computes the bounding box of the provided h3 cell considering edges to be great circles and
     * stores then in the provided {@link GeoBoundingBox}.
     */
    public static void computeGeoBounds(long h3, GeoBoundingBox boundingBox) {
        final CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        final int res = H3.getResolution(h3);
        if (h3 == H3.northPolarH3(res)) {
            // specialize north pole
            computeNorthPoleBounds(boundary, boundingBox);
        } else if (h3 == H3.southPolarH3(res)) {
            // specialize south pole
            computeSouthPoleBounds(boundary, boundingBox);
        } else {
            // generic case
            computeBounds(boundary, boundingBox);
        }
    }

    private static void computeNorthPoleBounds(CellBoundary boundary, GeoBoundingBox boundingBox) {
        double minLat = Double.POSITIVE_INFINITY;
        for (int i = 0; i < boundary.numPoints(); i++) {
            minLat = Math.min(minLat, boundary.getLatLon(i).getLatRad());
        }
        boundingBox.topLeft().reset(90, -180d);
        boundingBox.bottomRight().reset(Math.toDegrees(minLat), 180d);
    }

    private static void computeSouthPoleBounds(CellBoundary boundary, GeoBoundingBox boundingBox) {
        double maxLat = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < boundary.numPoints(); i++) {
            maxLat = Math.max(maxLat, boundary.getLatLon(i).getLatRad());
        }
        boundingBox.topLeft().reset(Math.toDegrees(maxLat), -180d);
        boundingBox.bottomRight().reset(-90, 180d);
    }

    private static void computeBounds(CellBoundary boundary, GeoBoundingBox boundingBox) {
        // This algorithm is based on the bounding box for great circle edges in
        // https://trs.jpl.nasa.gov/bitstream/handle/2014/41271/07-0286.pdf
        double minLat = Double.POSITIVE_INFINITY;
        int minLatPos = -1;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        int maxLatPos = -1;
        double maxLon = Double.NEGATIVE_INFINITY;
        double maxNegLon = Double.NEGATIVE_INFINITY;
        double minPosLon = Double.POSITIVE_INFINITY;
        for (int i = 0; i < boundary.numPoints(); i++) {
            final double lon = boundary.getLatLon(i).getLonRad();
            final double lat = boundary.getLatLon(i).getLatRad();
            if (lat < minLat) {
                minLat = lat;
                minLatPos = i;
            }
            if (lat > maxLat) {
                maxLat = lat;
                maxLatPos = i;
            }
            minLon = Math.min(minLon, lon);
            maxLon = Math.max(maxLon, lon);
            if (lon < 0) {
                maxNegLon = Math.max(maxNegLon, lon);
            } else {
                minPosLon = Math.min(minPosLon, lon);
            }
        }
        if (minLat < 0) {
            // we only correct the min latitude if negative
            minLat = boundary.getLatLon(minLatPos).greatCircleMinLatitude(computeEdge(boundary, minLatPos, MIN_COMPARATOR));
        }
        if (maxLat > 0) {
            // we only correct the max latitude if positive
            maxLat = boundary.getLatLon(maxLatPos).greatCircleMaxLatitude(computeEdge(boundary, maxLatPos, MAX_COMPARATOR));
        }
        // the min / max longitude is computed the same way as in cartesian, being careful with polygons crossing the dateline
        final boolean crossesDateline = maxLon - minLon > Math.PI;
        boundingBox.topLeft().reset(Math.toDegrees(maxLat), crossesDateline ? Math.toDegrees(minPosLon) : Math.toDegrees(minLon));
        boundingBox.bottomRight().reset(Math.toDegrees(minLat), crossesDateline ? Math.toDegrees(maxNegLon) : Math.toDegrees(maxLon));
    }

    private static LatLng computeEdge(CellBoundary boundary, int pos, BiPredicate<LatLng, LatLng> comparator) {
        final LatLng end1 = boundary.getLatLon((pos + 1) % boundary.numPoints());
        final LatLng end2 = boundary.getLatLon(pos == 0 ? boundary.numPoints() - 1 : pos - 1);
        return comparator.test(end1, end2) ? end1 : end2;
    }

    /** Return the {@link GeoPolygon} representing the provided H3 bin */
    public static GeoPolygon toGeoPolygon(long h3) {
        final CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        List<GeoPoint> points = new ArrayList<>(boundary.numPoints());
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng latLng = boundary.getLatLon(i);
            points.add(new GeoPoint(PlanetModel.SPHERE, latLng.getLatRad(), latLng.getLonRad()));
        }
        return GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    }

    /** Return the {@link LatLonGeometry} representing the provided H3 bin */
    public static LatLonGeometry getLatLonGeometry(long h3) {
        return new H3SphericalGeometry(h3);
    }
}
