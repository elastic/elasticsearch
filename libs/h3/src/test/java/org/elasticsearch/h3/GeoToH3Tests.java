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

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.test.ESTestCase;

public class GeoToH3Tests extends ESTestCase {

    public void testRandomPoints() {
        for (int i = 0; i < 50; i++) {
            // avoid points close to the poles
            double lat = randomValueOtherThanMany(d -> d > 60 || d < -60, GeoTestUtil::nextLatitude);
            // avoid points close to the dateline
            double lon = randomValueOtherThanMany(d -> d > 150 || d < -150, GeoTestUtil::nextLongitude);
            testPoint(lat, lon);
        }
    }

    private void testPoint(double lat, double lon) {
        for (int i = 0; i < Constants.MAX_H3_RES; i++) {
            String h3Address = H3.geoToH3Address(lat, lon, i);
            CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3Address);
            double minLat = cellBoundary.getLatLon(0).getLatDeg();
            double maxLat = cellBoundary.getLatLon(0).getLatDeg();
            double minLon = cellBoundary.getLatLon(0).getLonDeg();
            double maxLon = cellBoundary.getLatLon(0).getLonDeg();
            for (int j = 0; j < cellBoundary.numPoints(); j++) {
                minLat = Math.min(minLat, cellBoundary.getLatLon(j).getLatDeg());
                maxLat = Math.max(maxLat, cellBoundary.getLatLon(j).getLatDeg());
                minLon = Math.min(minLon, cellBoundary.getLatLon(j).getLonDeg());
                maxLon = Math.max(maxLon, cellBoundary.getLatLon(j).getLonDeg());
            }
            assertTrue(minLat <= lat);
            assertTrue(maxLat >= lat);
            assertTrue(minLon <= lon);
            assertTrue(maxLon >= lon);
        }
    }
}
