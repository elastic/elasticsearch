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
import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

public class GeoToH3Tests extends ESTestCase {

    public void testRandomPoints() {
        for (int i = 0; i < 50; i++) {
            testPoint(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude());
        }
    }

    private void testPoint(double lat, double lon) {
        GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Math.toRadians(lat), Math.toRadians(lon));
        for (int res = 0; res < Constants.MAX_H3_RES; res++) {
            String h3Address = H3.geoToH3Address(lat, lon, res);
            assertEquals(res, H3.getResolution(h3Address));
            GeoPolygon polygon = getGeoPolygon(h3Address);
            assertTrue(polygon.isWithin(point));
        }
    }

    private GeoPolygon getGeoPolygon(String h3Address) {
        CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3Address);
        List<GeoPoint> points = new ArrayList<>(cellBoundary.numPoints());
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            LatLng latLng = cellBoundary.getLatLon(i);
            points.add(new GeoPoint(PlanetModel.SPHERE, latLng.getLatRad(), latLng.getLonRad()));
        }
        return GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    }

    public void testNorthPoleCells() {
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            assertEquals(H3.northPolarH3Address(res), H3.geoToH3Address(90, GeoTestUtil.nextLongitude(), res));
        }
    }

    public void testSouthPoleCells() {
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            assertEquals(H3.southPolarH3Address(res), H3.geoToH3Address(-90, GeoTestUtil.nextLongitude(), res));
        }
    }
}
