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

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.MAX_ZOOM;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.checkPrecisionRange;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.hashToGeoPoint;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.keyToGeoPoint;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.longEncode;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.stringEncode;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;

public class GeoTileUtilsTests extends ESTestCase {

    private static final double GEOTILE_TOLERANCE = 1E-5D;

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
        assertEquals(0x0000000000000000L, longEncode(0, 0, 0));
        assertEquals(0x3C00095540001CA5L, longEncode(30, 70, 15));
        assertEquals(0x77FFFF4580000000L, longEncode(179.999, 89.999, 29));
        assertEquals(0x740000BA7FFFFFFFL, longEncode(-179.999, -89.999, 29));
        assertEquals(0x0800000040000001L, longEncode(1, 1, 2));
        assertEquals(0x0C00000060000000L, longEncode(-20, 100, 3));
        assertEquals(0x71127D27C8ACA67AL, longEncode(13, -15, 28));
        assertEquals(0x4C0077776003A9ACL, longEncode(-12, 15, 19));
        assertEquals(0x140000024000000EL, longEncode(-328.231870,16.064082, 5));
        assertEquals(0x6436F96B60000000L, longEncode(-590.769588,89.549167, 25));
        assertEquals(0x6411BD6BA0A98359L, longEncode(999.787079,51.830093, 25));
        assertEquals(0x751BD6BBCA983596L, longEncode(999.787079,51.830093, 29));
        assertEquals(0x77CF880A20000000L, longEncode(-557.039740,-632.103969, 29));
        assertEquals(0x7624FA4FA0000000L, longEncode(13,88, 29));
        assertEquals(0x7624FA4FBFFFFFFFL, longEncode(13,-88, 29));
        assertEquals(0x0400000020000000L, longEncode(13,89, 1));
        assertEquals(0x0400000020000001L, longEncode(13,-89, 1));
        assertEquals(0x0400000020000000L, longEncode(13,95, 1));
        assertEquals(0x0400000020000001L, longEncode(13,-95, 1));

        expectThrows(IllegalArgumentException.class, () -> longEncode(0, 0, -1));
        expectThrows(IllegalArgumentException.class, () -> longEncode(-1, 0, MAX_ZOOM + 1));
    }

    public void testLongEncodeFromString() {
        assertEquals(0x0000000000000000L, longEncode(stringEncode(longEncode(0, 0, 0))));
        assertEquals(0x3C00095540001CA5L, longEncode(stringEncode(longEncode(30, 70, 15))));
        assertEquals(0x77FFFF4580000000L, longEncode(stringEncode(longEncode(179.999, 89.999, 29))));
        assertEquals(0x740000BA7FFFFFFFL, longEncode(stringEncode(longEncode(-179.999, -89.999, 29))));
        assertEquals(0x0800000040000001L, longEncode(stringEncode(longEncode(1, 1, 2))));
        assertEquals(0x0C00000060000000L, longEncode(stringEncode(longEncode(-20, 100, 3))));
        assertEquals(0x71127D27C8ACA67AL, longEncode(stringEncode(longEncode(13, -15, 28))));
        assertEquals(0x4C0077776003A9ACL, longEncode(stringEncode(longEncode(-12, 15, 19))));
        assertEquals(0x140000024000000EL, longEncode(stringEncode(longEncode(-328.231870,16.064082, 5))));
        assertEquals(0x6436F96B60000000L, longEncode(stringEncode(longEncode(-590.769588,89.549167, 25))));
        assertEquals(0x6411BD6BA0A98359L, longEncode(stringEncode(longEncode(999.787079,51.830093, 25))));
        assertEquals(0x751BD6BBCA983596L, longEncode(stringEncode(longEncode(999.787079,51.830093, 29))));
        assertEquals(0x77CF880A20000000L, longEncode(stringEncode(longEncode(-557.039740,-632.103969, 29))));
        assertEquals(0x7624FA4FA0000000L, longEncode(stringEncode(longEncode(13,88, 29))));
        assertEquals(0x7624FA4FBFFFFFFFL, longEncode(stringEncode(longEncode(13,-88, 29))));
        assertEquals(0x0400000020000000L, longEncode(stringEncode(longEncode(13,89, 1))));
        assertEquals(0x0400000020000001L, longEncode(stringEncode(longEncode(13,-89, 1))));
        assertEquals(0x0400000020000000L, longEncode(stringEncode(longEncode(13,95, 1))));
        assertEquals(0x0400000020000001L, longEncode(stringEncode(longEncode(13,-95, 1))));

        expectThrows(IllegalArgumentException.class, () -> longEncode("12/asdf/1"));
        expectThrows(IllegalArgumentException.class, () -> longEncode("foo"));
    }

    private void assertGeoPointEquals(GeoPoint gp, final double longitude, final double latitude) {
        assertThat(gp.lon(), closeTo(longitude, GEOTILE_TOLERANCE));
        assertThat(gp.lat(), closeTo(latitude, GEOTILE_TOLERANCE));
    }

    public void testHashToGeoPoint() {
        assertGeoPointEquals(keyToGeoPoint("0/0/0"), 0.0, 0.0);
        assertGeoPointEquals(keyToGeoPoint("1/0/0"), -90.0, 66.51326044311186);
        assertGeoPointEquals(keyToGeoPoint("1/1/0"), 90.0, 66.51326044311186);
        assertGeoPointEquals(keyToGeoPoint("1/0/1"), -90.0, -66.51326044311186);
        assertGeoPointEquals(keyToGeoPoint("1/1/1"), 90.0, -66.51326044311186);
        assertGeoPointEquals(keyToGeoPoint("29/536870000/10"), 179.99938879162073, 85.05112817241982);
        assertGeoPointEquals(keyToGeoPoint("29/10/536870000"), -179.99999295920134, -85.0510760525731);

        //noinspection ConstantConditions
        expectThrows(NullPointerException.class, () -> keyToGeoPoint(null));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint(""));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("a"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("0"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("0/0"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("0/0/0/0"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("0/-1/-1"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("0/-1/0"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("0/0/-1"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("a/0/0"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("0/a/0"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("0/0/a"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint("-1/0/0"));
        expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint((MAX_ZOOM + 1) + "/0/0"));

        for (int z = 0; z <= MAX_ZOOM; z++) {
            final int zoom = z;
            final int max_index = (int) Math.pow(2, zoom);
            expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint(zoom + "/0/" + max_index));
            expectThrows(IllegalArgumentException.class, () -> keyToGeoPoint(zoom + "/" + max_index + "/0"));
        }
    }

    /**
     * Make sure that hash produces the expected key, and that the key could be converted to hash via a GeoPoint
     */
    private void assertStrCodec(long hash, String key, int zoom) {
        assertEquals(key, stringEncode(hash));
        final GeoPoint gp = keyToGeoPoint(key);
        assertEquals(hash, longEncode(gp.lon(), gp.lat(), zoom));
    }

    /**
     * A few hardcoded lat/lng/zoom hashing expectations
     */
    public void testStringEncode() {
        assertStrCodec(0x0000000000000000L, "0/0/0", 0);
        assertStrCodec(0x3C00095540001CA5L, "15/19114/7333", 15);
        assertStrCodec(0x77FFFF4580000000L, "29/536869420/0", 29);
        assertStrCodec(0x740000BA7FFFFFFFL, "29/1491/536870911", 29);
        assertStrCodec(0x0800000040000001L, "2/2/1", 2);
        assertStrCodec(0x0C00000060000000L, "3/3/0", 3);
        assertStrCodec(0x71127D27C8ACA67AL, "28/143911230/145532538", 28);
        assertStrCodec(0x4C0077776003A9ACL, "19/244667/240044", 19);
        assertStrCodec(0x140000024000000EL, "5/18/14", 5);
        assertStrCodec(0x6436F96B60000000L, "25/28822363/0", 25);
        assertStrCodec(0x6411BD6BA0A98359L, "25/9300829/11109209", 25);
        assertStrCodec(0x751BD6BBCA983596L, "29/148813278/177747350", 29);
        assertStrCodec(0x77CF880A20000000L, "29/511459409/0", 29);
        assertStrCodec(0x7624FA4FA0000000L, "29/287822461/0", 29);
        assertStrCodec(0x7624FA4FBFFFFFFFL, "29/287822461/536870911", 29);
        assertStrCodec(0x0400000020000000L, "1/1/0", 1);
        assertStrCodec(0x0400000020000001L, "1/1/1", 1);

        expectThrows(IllegalArgumentException.class, () -> stringEncode(-1L));
        expectThrows(IllegalArgumentException.class, () -> stringEncode(0x7800000000000000L)); // z=30
        expectThrows(IllegalArgumentException.class, () -> stringEncode(0x0000000000000001L)); // z=0,x=0,y=1
        expectThrows(IllegalArgumentException.class, () -> stringEncode(0x0000000020000000L)); // z=0,x=1,y=0

        for (int zoom = 0; zoom < 5; zoom++) {
            int maxTile = 1 << zoom;
            for (int x = 0; x < maxTile; x++) {
                for (int y = 0; y < maxTile; y++) {
                    String expectedTileIndex = zoom + "/" + x + "/" + y;
                    GeoPoint point = keyToGeoPoint(expectedTileIndex);
                    String actualTileIndex = stringEncode(longEncode(point.lon(), point.lat(), zoom));
                    assertEquals(expectedTileIndex, actualTileIndex);
                }
            }
        }
    }

    /**
     * Ensure that for all points at all supported precision levels that the long encoding of a geotile
     * is compatible with its String based counterpart
     */
    public void testGeoTileAsLongRoutines() {
        for (double lat = -90; lat <= 90; lat++) {
            for (double lng = -180; lng <= 180; lng++) {
                for (int p = 0; p <= 29; p++) {
                    long hash = longEncode(lng, lat, p);
                    if (p > 0) {
                        assertNotEquals(0, hash);
                    }

                    // GeoPoint would be in the center of the bucket, thus must produce the same hash
                    GeoPoint point = hashToGeoPoint(hash);
                    long hashAsLong2 = longEncode(point.lon(), point.lat(), p);
                    assertEquals(hash, hashAsLong2);

                    // Same point should be generated from the string key
                    assertEquals(point, keyToGeoPoint(stringEncode(hash)));
                }
            }
        }
    }

    /**
     * Make sure the polar regions are handled properly.
     * Mercator projection does not show anything above 85 or below -85,
     * so ensure they are clipped correctly.
     */
    public void testSingularityAtPoles() {
        double minLat = -85.05112878;
        double maxLat = 85.05112878;
        double lon = randomIntBetween(-180, 180);
        double lat = randomBoolean()
            ? randomDoubleBetween(-90, minLat, true)
            : randomDoubleBetween(maxLat, 90, true);
        double clippedLat = Math.min(Math.max(lat, minLat), maxLat);
        int zoom = randomIntBetween(0, MAX_ZOOM);
        String tileIndex = stringEncode(longEncode(lon, lat, zoom));
        String clippedTileIndex = stringEncode(longEncode(lon, clippedLat, zoom));
        assertEquals(tileIndex, clippedTileIndex);
    }
}
