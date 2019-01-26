/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.geo.GeoTileUtils.checkPrecisionRange;
import static org.elasticsearch.common.geo.GeoTileUtils.hashToGeoPoint;
import static org.elasticsearch.common.geo.GeoTileUtils.longEncode;
import static org.elasticsearch.common.geo.GeoTileUtils.stringEncode;
import static org.hamcrest.Matchers.containsString;

public class GeoTileUtilsTests extends ESTestCase {

    /**
     * Precision validation should throw an error if its outside of the valid range.
     */
    public void testCheckPrecisionRange() {
        for (int i = 0; i <= 29; i++) {
            assertEquals(i, checkPrecisionRange(i));
        }
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> checkPrecisionRange(-1));
        assertThat(ex.getMessage(), containsString("Invalid geotile_grid precision of -1. Must be between 0 and 29."));
        ex = expectThrows(IllegalArgumentException.class, () -> checkPrecisionRange(30));
        assertThat(ex.getMessage(), containsString("Invalid geotile_grid precision of 30. Must be between 0 and 29."));
    }

    /**
     * A few hardcoded lat/lng/zoom hashing expectations
     */
    public void testLongEncode() {
        assertEquals(0, longEncode(0, 0, 0));
        assertEquals(0x3c00000012e4cc66L, longEncode(30, 70, 15));
        assertEquals(0x7555555555440450L, longEncode(179.999, 89.999, 29));
        assertEquals(0x76aaaaaaaabbfbafL, longEncode(-179.999, -89.999, 29));
        assertEquals(0x0800000000000006L, longEncode(1, 1, 2));
        assertEquals(0x0c00000000000005L, longEncode(-20, 100, 3));
        assertEquals(0x70c0c9a5dc692fdcL, longEncode(13, -15, 28));
        assertEquals(0x4c00000fcdc7cde5L, longEncode(-12, 15, 19));
    }

    /**
     * Ensure that for all points at all supported precision levels that the long encoding of a geotile
     * is compatible with its String based counterpart
     */
    public void testGeoTileAsLongRoutines() {
        for (double lat=-90; lat<=90; lat++) {
            for (double lng=-180; lng<=180; lng++) {
                for(int p=0; p<=29; p++) {
                    long hash = longEncode(lng, lat, p);
                    if (p > 0) {
                        assertNotEquals(0, hash);
                    }

                    // GeoPoint would be in the center of the bucket, thus must produce the same hash
                    GeoPoint point = hashToGeoPoint(hash);
                    long hashAsLong2 = longEncode(point.lon(), point.lat(), p);
                    assertEquals(hash, hashAsLong2);

                    // Same point should be generated from the string key
                    assertEquals(point, hashToGeoPoint(stringEncode(hash)));
                }
            }
        }
    }
}
