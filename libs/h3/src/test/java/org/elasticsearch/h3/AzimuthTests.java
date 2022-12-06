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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/92136")
    public void testLatLonVec3d() {
        final double lat = Math.toRadians(GeoTestUtil.nextLatitude());
        final double lon = Math.toRadians(GeoTestUtil.nextLongitude());
        final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, lat, lon);
        for (int i = 0; i < Vec3d.faceCenterPoint.length; i++) {
            final double azVec3d = Vec3d.faceCenterPoint[i].geoAzimuthRads(point.x, point.y, point.z);
            final double azVec2d = Vec2d.faceCenterGeo[i].geoAzimuthRads(point.getLatitude(), point.getLongitude());
            assertEquals(azVec2d, azVec3d, 1e-14);
        }
    }
}
