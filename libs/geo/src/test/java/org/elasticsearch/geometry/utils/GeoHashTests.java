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
package org.elasticsearch.geometry.utils;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

/**
 * Tests for {@link Geohash}
 */
public class GeoHashTests extends ESTestCase {
    public void testGeohashAsLongRoutines() {
        final GeoPoint expected = new GeoPoint();
        final GeoPoint actual = new GeoPoint();
        //Ensure that for all points at all supported levels of precision
        // that the long encoding of a geohash is compatible with its
        // String based counterpart
        for (double lat=-90;lat<90;lat++)
        {
            for (double lng=-180;lng<180;lng++)
            {
                for(int p=1;p<=12;p++)
                {
                    long geoAsLong = Geohash.longEncode(lng, lat, p);

                    // string encode from geohashlong encoded location
                    String geohashFromLong = Geohash.stringEncode(geoAsLong);

                    // string encode from full res lat lon
                    String geohash = Geohash.stringEncode(lng, lat, p);

                    // ensure both strings are the same
                    assertEquals(geohash, geohashFromLong);

                    // decode from the full-res geohash string
                    expected.resetFromGeoHash(geohash);
                    // decode from the geohash encoded long
                    actual.resetFromGeoHash(geoAsLong);

                    assertEquals(expected, actual);
                }
            }
        }
    }

    public void testBboxFromHash() {
        String hash = randomGeohash(1, 12);
        int level = hash.length();
        Rectangle bbox = Geohash.toBoundingBox(hash);
        // check that the length is as expected
        double expectedLonDiff = 360.0 / (Math.pow(8.0, (level + 1) / 2) * Math.pow(4.0, level / 2));
        double expectedLatDiff = 180.0 / (Math.pow(4.0, (level + 1) / 2) * Math.pow(8.0, level / 2));
        assertEquals(expectedLonDiff, bbox.getMaxX() - bbox.getMinX(), 0.00001);
        assertEquals(expectedLatDiff, bbox.getMaxY() - bbox.getMinY(), 0.00001);
        assertEquals(hash, Geohash.stringEncode(bbox.getMinX(), bbox.getMinY(), level));
    }

    public void testGeohashExtremes() {
        assertEquals("000000000000", Geohash.stringEncode(-180, -90));
        assertEquals("800000000000", Geohash.stringEncode(-180, 0));
        assertEquals("bpbpbpbpbpbp", Geohash.stringEncode(-180, 90));
        assertEquals("h00000000000", Geohash.stringEncode(0, -90));
        assertEquals("s00000000000", Geohash.stringEncode(0, 0));
        assertEquals("upbpbpbpbpbp", Geohash.stringEncode(0, 90));
        assertEquals("pbpbpbpbpbpb", Geohash.stringEncode(180, -90));
        assertEquals("xbpbpbpbpbpb", Geohash.stringEncode(180, 0));
        assertEquals("zzzzzzzzzzzz", Geohash.stringEncode(180, 90));
    }

    public void testLongGeohashes() {
        for (int i = 0; i < 100000; i++) {
            String geohash = randomGeohash(12, 12);
            GeoPoint expected = GeoPoint.fromGeohash(geohash);
            // Adding some random geohash characters at the end
            String extendedGeohash = geohash + randomGeohash(1, 10);
            GeoPoint actual = GeoPoint.fromGeohash(extendedGeohash);
            assertEquals("Additional data points above 12 should be ignored [" + extendedGeohash + "]" , expected, actual);

            Rectangle expectedBbox = Geohash.toBoundingBox(geohash);
            Rectangle actualBbox = Geohash.toBoundingBox(extendedGeohash);
            assertEquals("Additional data points above 12 should be ignored [" + extendedGeohash + "]" , expectedBbox, actualBbox);
        }
    }

    public void testNorthPoleBoundingBox() {
        Rectangle bbox = Geohash.toBoundingBox("zzbxfpgzupbx"); // Bounding box with maximum precision touching north pole
        assertEquals(90.0, bbox.getMaxY(), 0.0000001); // Should be 90 degrees
    }

    public void testInvalidGeohashes() {
        IllegalArgumentException ex;

        ex = expectThrows(IllegalArgumentException.class, () -> Geohash.mortonEncode("55.5"));
        assertEquals("unsupported symbol [.] in geohash [55.5]", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> Geohash.mortonEncode(""));
        assertEquals("empty geohash", ex.getMessage());
    }

}
