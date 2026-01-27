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
import org.elasticsearch.h3.H3;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.common.H3SphericalUtil;

/**
 * TODO: This class is a copy of the same class in org.elasticsearch.xpack.spatial.common, we should find a common location for it.
 */
public class H3SphericalUtilTests extends ESTestCase {

    private static final double LAT_DELTA = 1e-7;
    private static final double LON_DELTA = 1e-5;

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
