/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.LatLonBounds;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.common.H3SphericalUtil;

/**
 * TODO: This class is a copy of the same class in org.elasticsearch.xpack.spatial.common, we should find a common location for it.
 */
public class H3SphericalUtilTests extends ESTestCase {

    private static final double LAT_DELTA = 1e-7;
    // Lucene's spatial3d polygon bounds use 3D Cartesian coordinates internally and can differ from vertex-based
    // lon bounds by up to ~3e-5 degrees due to coordinate conversion precision; 1e-4 provides comfortable margin.
    private static final double LON_DELTA = 1e-4;

    public void testRandomBounds() {
        GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(), new GeoPoint());
        for (int res = 0; res < H3.MAX_H3_RES; res++) {
            final long h3 = H3.geoToH3(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude(), res);
            assertBounds(h3, boundingBox);
        }
    }

    public void testBoundsLevel0() {
        GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(), new GeoPoint());
        for (long h3 : H3.getLongRes0Cells()) {
            assertBounds(h3, boundingBox);
        }
    }

    /**
     * Explicitly verifies that when correcting the latitude bounds for great-circle arcs, both
     * edges adjacent to the extreme-latitude vertex are checked and the more extreme result is
     * used. The heuristic of selecting the edge based solely on the neighbor's vertex latitude
     * can pick the wrong edge — the neighbor with the less extreme vertex latitude can still
     * produce a great-circle arc that achieves a more extreme latitude. Resolution-0 cells have
     * the longest edges and are therefore the most likely to exhibit this difference.
     */
    public void testBothEdgesCheckedForExtremeLatitudeCorrection() {
        GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(), new GeoPoint());
        for (long h3 : H3.getLongRes0Cells()) {
            final int res = H3.getResolution(h3);
            if (h3 == H3.northPolarH3(res) || h3 == H3.southPolarH3(res)) {
                continue; // polar cells use a separate code path
            }
            CellBoundary boundary = H3.h3ToGeoBoundary(h3);
            int numPoints = boundary.numPoints();
            int maxLatPos = 0, minLatPos = 0;
            for (int i = 1; i < numPoints; i++) {
                if (boundary.getLatLon(i).getLatRad() > boundary.getLatLon(maxLatPos).getLatRad()) {
                    maxLatPos = i;
                }
                if (boundary.getLatLon(i).getLatRad() < boundary.getLatLon(minLatPos).getLatRad()) {
                    minLatPos = i;
                }
            }
            org.elasticsearch.xpack.spatial.common.H3SphericalUtil.computeGeoBounds(h3, boundingBox);

            LatLng vMax = boundary.getLatLon(maxLatPos);
            if (vMax.getLatRad() > 0) {
                LatLng maxN1 = boundary.getLatLon((maxLatPos + 1) % numPoints);
                LatLng maxN2 = boundary.getLatLon(maxLatPos == 0 ? numPoints - 1 : maxLatPos - 1);
                double expectedMaxLat = Math.max(vMax.greatCircleMaxLatitude(maxN1), vMax.greatCircleMaxLatitude(maxN2));
                assertEquals("max lat for H3 cell " + Long.toHexString(h3), Math.toDegrees(expectedMaxLat), boundingBox.top(), LAT_DELTA);
            }

            LatLng vMin = boundary.getLatLon(minLatPos);
            if (vMin.getLatRad() < 0) {
                LatLng minN1 = boundary.getLatLon((minLatPos + 1) % numPoints);
                LatLng minN2 = boundary.getLatLon(minLatPos == 0 ? numPoints - 1 : minLatPos - 1);
                double expectedMinLat = Math.min(vMin.greatCircleMinLatitude(minN1), vMin.greatCircleMinLatitude(minN2));
                assertEquals(
                    "min lat for H3 cell " + Long.toHexString(h3),
                    Math.toDegrees(expectedMinLat),
                    boundingBox.bottom(),
                    LAT_DELTA
                );
            }
        }
    }

    private void assertBounds(long h3, GeoBoundingBox boundingBox) {
        org.elasticsearch.xpack.spatial.common.H3SphericalUtil.computeGeoBounds(h3, boundingBox);
        GeoPolygon polygon = H3SphericalUtil.toGeoPolygon(h3);
        LatLonBounds bounds = new LatLonBounds();
        polygon.getBounds(bounds);
        if (bounds.checkNoLongitudeBound()) {
            assertEquals(-180d, boundingBox.left(), LON_DELTA);
            assertEquals(180d, boundingBox.right(), LON_DELTA);
        } else {
            assertEquals(Math.toDegrees(bounds.getLeftLongitude()), boundingBox.left(), LON_DELTA);
            assertEquals(Math.toDegrees(bounds.getRightLongitude()), boundingBox.right(), LON_DELTA);
        }

        if (bounds.checkNoTopLatitudeBound()) {
            assertEquals(90d, boundingBox.top(), LAT_DELTA);
        } else {
            assertEquals(Math.toDegrees(bounds.getMaxLatitude()), boundingBox.top(), LAT_DELTA);
        }

        if (bounds.checkNoBottomLatitudeBound()) {
            assertEquals(-90d, boundingBox.bottom(), LAT_DELTA);
        } else {
            assertEquals(Math.toDegrees(bounds.getMinLatitude()), boundingBox.bottom(), LAT_DELTA);
        }
    }
}
