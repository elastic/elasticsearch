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

package org.elasticsearch.index.search.geo;

import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class GeoHashUtilsTests extends ElasticsearchTestCase {

    /**
     * Pass condition: lat=42.6, lng=-5.6 should be encoded as "ezs42e44yx96",
     * lat=57.64911 lng=10.40744 should be encoded as "u4pruydqqvj8"
     */
    @Test
    public void testEncode() {
        String hash = GeoHashUtils.encode(42.6, -5.6);
        assertEquals("ezs42e44yx96", hash);

        hash = GeoHashUtils.encode(57.64911, 10.40744);
        assertEquals("u4pruydqqvj8", hash);
    }

    /**
     * Pass condition: lat=52.3738007, lng=4.8909347 should be encoded and then
     * decoded within 0.00001 of the original value
     */
    @Test
    public void testDecodePreciseLongitudeLatitude() {
        String hash = GeoHashUtils.encode(52.3738007, 4.8909347);

        GeoPoint point = GeoHashUtils.decode(hash);

        assertEquals(52.3738007, point.lat(), 0.00001D);
        assertEquals(4.8909347, point.lon(), 0.00001D);
    }

    /**
     * Pass condition: lat=84.6, lng=10.5 should be encoded and then decoded
     * within 0.00001 of the original value
     */
    @Test
    public void testDecodeImpreciseLongitudeLatitude() {
        String hash = GeoHashUtils.encode(84.6, 10.5);

        GeoPoint point = GeoHashUtils.decode(hash);

        assertEquals(84.6, point.lat(), 0.00001D);
        assertEquals(10.5, point.lon(), 0.00001D);
    }

    /*
    * see https://issues.apache.org/jira/browse/LUCENE-1815 for details
    */

    @Test
    public void testDecodeEncode() {
        String geoHash = "u173zq37x014";
        assertEquals(geoHash, GeoHashUtils.encode(52.3738007, 4.8909347));
        GeoPoint decode = GeoHashUtils.decode(geoHash);
        assertEquals(52.37380061d, decode.lat(), 0.000001d);
        assertEquals(4.8909343d, decode.lon(), 0.000001d);

        assertEquals(geoHash, GeoHashUtils.encode(decode.lat(), decode.lon()));
    }

    @Test
    public void testNeighbours() {
        String geohash = "gcpv";
        List<String> expectedNeighbors = new ArrayList<>();
        expectedNeighbors.add("gcpw");
        expectedNeighbors.add("gcpy");
        expectedNeighbors.add("u10n");
        expectedNeighbors.add("gcpt");
        expectedNeighbors.add("u10j");
        expectedNeighbors.add("gcps");
        expectedNeighbors.add("gcpu");
        expectedNeighbors.add("u10h");
        Collection<? super String> neighbors = new ArrayList<>();
        GeoHashUtils.addNeighbors(geohash, neighbors );
        assertEquals(expectedNeighbors, neighbors);
    }
}
