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

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class HexRingTests extends ESTestCase {

    public void testInvalidHexRingPos() {
        long h3 = H3.geoToH3(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude(), randomIntBetween(0, H3.MAX_H3_RES));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> H3.hexRingPosToH3(h3, -1));
        assertEquals(ex.getMessage(), "invalid ring position");
        int pos = H3.isPentagon(h3) ? 5 : 6;
        ex = expectThrows(IllegalArgumentException.class, () -> H3.hexRingPosToH3(h3, pos));
        assertEquals(ex.getMessage(), "invalid ring position");
    }

    public void testHexRing() {
        for (int i = 0; i < 500; i++) {
            double lat = GeoTestUtil.nextLatitude();
            double lon = GeoTestUtil.nextLongitude();
            for (int res = 0; res <= Constants.MAX_H3_RES; res++) {
                String origin = H3.geoToH3Address(lat, lon, res);
                assertFalse(H3.areNeighborCells(origin, origin));
                String[] ring = H3.hexRing(origin);
                Arrays.sort(ring);
                for (String destination : ring) {
                    assertTrue(H3.areNeighborCells(origin, destination));
                    String[] newRing = H3.hexRing(destination);
                    for (String newDestination : newRing) {
                        if (Arrays.binarySearch(ring, newDestination) >= 0) {
                            assertTrue(H3.areNeighborCells(origin, newDestination));
                        } else {
                            assertFalse(H3.areNeighborCells(origin, newDestination));
                        }
                    }

                }
            }
        }
    }
}
