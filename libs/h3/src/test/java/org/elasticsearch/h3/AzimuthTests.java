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
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.test.ESTestCase;

public class AzimuthTests extends ESTestCase {

    public void testLatLonVec3d() {
        final GeoPoint point = safePoint();
        for (int i = 0; i < Vec3d.faceCenterPoint.length; i++) {
            final double azVec3d = Vec3d.faceCenterPoint[i].geoAzimuthRads(point.x, point.y, point.z);
            final double azVec2d = Vec2d.faceCenterGeo[i].geoAzimuthRads(point.getLatitude(), point.getLongitude());
            assertEquals("Face " + i, azVec2d, azVec3d, 1e-12);
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
}
