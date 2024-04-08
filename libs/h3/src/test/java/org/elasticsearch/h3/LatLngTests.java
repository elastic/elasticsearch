/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.h3;

import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.LatLonBounds;
import org.apache.lucene.spatial3d.geom.Plane;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.SidedPlane;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.test.ESTestCase;

public class LatLngTests extends ESTestCase {

    public void testLatLonAzimuthRads() {
        final GeoPoint point = safePoint();
        for (int i = 0; i < Vec3d.faceCenterPoint.length; i++) {
            final double azVec3d = Vec3d.faceCenterPoint[i].geoAzimuthRads(point.x, point.y, point.z);
            final double azVec2d = Vec2d.faceCenterGeo[i].geoAzimuthRads(point.getLatitude(), point.getLongitude());
            assertEquals("Face " + i, azVec2d, azVec3d, 1e-12);

        }
    }

    public void testLatLonAzDistanceRads() {
        final double az = randomDoubleBetween(-2 * Math.PI, 2 * Math.PI, true);
        final double distance = randomDoubleBetween(-Math.PI, Math.PI / 2, true);
        for (int i = 0; i < Vec3d.faceCenterPoint.length; i++) {
            final LatLng latLng3d = Vec3d.faceCenterPoint[i].geoAzDistanceRads(az, distance);
            final LatLng latLng2d = Vec2d.faceCenterGeo[i].geoAzDistanceRads(az, distance);
            assertTrue("Face " + i, latLng2d.isNumericallyIdentical(latLng3d));
        }
    }

    /**
     * Face 19 gives -180 vs 180 for azimuth when Latitude=-90 and Longitude is between 98 and 102.
     * So we just exclude lat=-90 from the test to avoid this odd edge case.
     */
    private GeoPoint safePoint() {
        GeoPoint point;
        do {
            final double lat = Math.toRadians(GeoTestUtil.nextLatitude());
            final double lon = Math.toRadians(GeoTestUtil.nextLongitude());
            point = new GeoPoint(PlanetModel.SPHERE, lat, lon);
        } while (point.getLatitude() == -Math.PI / 2);
        return point;
    }

    public void testGreatCircleMaxMinLatitude() {
        for (int i = 0; i < 10; i++) {
            final GeoPoint point1 = safePoint();
            final GeoPoint point2 = safePoint();
            final LatLng latLng1 = new LatLng(point1.getLatitude(), point1.getLongitude());
            final LatLng latLng2 = new LatLng(point2.getLatitude(), point2.getLongitude());
            final LatLonBounds bounds = getBounds(point1, point2);
            assertEquals(bounds.getMaxLatitude(), latLng1.greatCircleMaxLatitude(latLng2), 1e-7);
            assertEquals(bounds.getMinLatitude(), latLng1.greatCircleMinLatitude(latLng2), 1e-7);
        }
    }

    private LatLonBounds getBounds(final GeoPoint point1, final GeoPoint point2) {
        final LatLonBounds bounds = new LatLonBounds();
        bounds.addPoint(point1);
        bounds.addPoint(point2);
        if (point1.isNumericallyIdentical(point2) == false) {
            final Plane plane = new Plane(point1, point2);
            bounds.addPlane(PlanetModel.SPHERE, plane, new SidedPlane(point1, plane, point2), new SidedPlane(point2, point1, plane));
        }
        return bounds;
    }

    public void testEqualsAndHashCode() {
        final LatLng latLng = new LatLng(0, 0);
        {
            LatLng otherLatLng = new LatLng(1, 1);
            assertNotEquals(latLng, otherLatLng);
            assertNotEquals(latLng.hashCode(), otherLatLng.hashCode());
        }
        {
            LatLng otherLatLng = new LatLng(0, 0);
            assertEquals(latLng, otherLatLng);
            assertEquals(latLng.hashCode(), otherLatLng.hashCode());
        }
    }
}
