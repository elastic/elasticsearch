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

package org.elasticsearch.geo;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

import java.util.Random;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoUtils.MAX_LAT_INCL;
import static org.apache.lucene.geo.GeoUtils.MAX_LON_INCL;
import static org.apache.lucene.geo.GeoUtils.MIN_LAT_INCL;
import static org.apache.lucene.geo.GeoUtils.MIN_LON_INCL;

/**
 * Tests methods in {@link GeoEncodingUtils}
 */
public class TestGeoEncodingUtils extends LuceneTestCase {

    /**
     * step through some integers, ensuring they decode to their expected double values.
     * double values start at -90 and increase by LATITUDE_DECODE for each integer.
     * check edge cases within the double range and random doubles within the range too.
     */
    public void testLatitudeQuantization() throws Exception {
        final double LATITUDE_DECODE = 180.0D / (0x1L << 32);
        Random random = random();
        for (int i = 0; i < 10000; i++) {
            int encoded = random.nextInt();
            double min = MIN_LAT_INCL + (encoded - (long) Integer.MIN_VALUE) * LATITUDE_DECODE;
            double decoded = decodeLatitude(encoded);
            // should exactly equal expected value
            assertEquals(min, decoded, 0.0D);
            // should round-trip
            assertEquals(encoded, encodeLatitude(decoded));
            assertEquals(encoded, encodeLatitudeCeil(decoded));
            // test within the range
            if (encoded != Integer.MAX_VALUE) {
                // this is the next representable value
                // all double values between [min .. max) should encode to the current integer
                // all double values between (min .. max] should encodeCeil to the next integer.
                double max = min + LATITUDE_DECODE;
                assertEquals(max, decodeLatitude(encoded + 1), 0.0D);
                assertEquals(encoded + 1, encodeLatitude(max));
                assertEquals(encoded + 1, encodeLatitudeCeil(max));

                // first and last doubles in range that will be quantized
                double minEdge = Math.nextUp(min);
                double maxEdge = Math.nextDown(max);
                assertEquals(encoded, encodeLatitude(minEdge));
                assertEquals(encoded + 1, encodeLatitudeCeil(minEdge));
                assertEquals(encoded, encodeLatitude(maxEdge));
                assertEquals(encoded + 1, encodeLatitudeCeil(maxEdge));

                // check random values within the double range
                long minBits = NumericUtils.doubleToSortableLong(minEdge);
                long maxBits = NumericUtils.doubleToSortableLong(maxEdge);
                for (int j = 0; j < 100; j++) {
                    double value = NumericUtils.sortableLongToDouble(TestUtil.nextLong(random, minBits, maxBits));
                    // round down
                    assertEquals(encoded, encodeLatitude(value));
                    // round up
                    assertEquals(encoded + 1, encodeLatitudeCeil(value));
                }
            }
        }
    }

    /**
     * step through some integers, ensuring they decode to their expected double values.
     * double values start at -180 and increase by LONGITUDE_DECODE for each integer.
     * check edge cases within the double range and a random doubles within the range too.
     */
    public void testLongitudeQuantization() throws Exception {
        final double LONGITUDE_DECODE = 360.0D / (0x1L << 32);
        Random random = random();
        for (int i = 0; i < 10000; i++) {
            int encoded = random.nextInt();
            double min = MIN_LON_INCL + (encoded - (long) Integer.MIN_VALUE) * LONGITUDE_DECODE;
            double decoded = decodeLongitude(encoded);
            // should exactly equal expected value
            assertEquals(min, decoded, 0.0D);
            // should round-trip
            assertEquals(encoded, encodeLongitude(decoded));
            assertEquals(encoded, encodeLongitudeCeil(decoded));
            // test within the range
            if (encoded != Integer.MAX_VALUE) {
                // this is the next representable value
                // all double values between [min .. max) should encode to the current integer
                // all double values between (min .. max] should encodeCeil to the next integer.
                double max = min + LONGITUDE_DECODE;
                assertEquals(max, decodeLongitude(encoded + 1), 0.0D);
                assertEquals(encoded + 1, encodeLongitude(max));
                assertEquals(encoded + 1, encodeLongitudeCeil(max));

                // first and last doubles in range that will be quantized
                double minEdge = Math.nextUp(min);
                double maxEdge = Math.nextDown(max);
                assertEquals(encoded, encodeLongitude(minEdge));
                assertEquals(encoded + 1, encodeLongitudeCeil(minEdge));
                assertEquals(encoded, encodeLongitude(maxEdge));
                assertEquals(encoded + 1, encodeLongitudeCeil(maxEdge));

                // check random values within the double range
                long minBits = NumericUtils.doubleToSortableLong(minEdge);
                long maxBits = NumericUtils.doubleToSortableLong(maxEdge);
                for (int j = 0; j < 100; j++) {
                    double value = NumericUtils.sortableLongToDouble(TestUtil.nextLong(random, minBits, maxBits));
                    // round down
                    assertEquals(encoded, encodeLongitude(value));
                    // round up
                    assertEquals(encoded + 1, encodeLongitudeCeil(value));
                }
            }
        }
    }

    // check edge/interesting cases explicitly
    public void testEncodeEdgeCases() {
        assertEquals(Integer.MIN_VALUE, encodeLatitude(MIN_LAT_INCL));
        assertEquals(Integer.MIN_VALUE, encodeLatitudeCeil(MIN_LAT_INCL));
        assertEquals(Integer.MAX_VALUE, encodeLatitude(MAX_LAT_INCL));
        assertEquals(Integer.MAX_VALUE, encodeLatitudeCeil(MAX_LAT_INCL));

        assertEquals(Integer.MIN_VALUE, encodeLongitude(MIN_LON_INCL));
        assertEquals(Integer.MIN_VALUE, encodeLongitudeCeil(MIN_LON_INCL));
        assertEquals(Integer.MAX_VALUE, encodeLongitude(MAX_LON_INCL));
        assertEquals(Integer.MAX_VALUE, encodeLongitudeCeil(MAX_LON_INCL));
    }
}
