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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.elasticsearch.common.geo.GeoHashUtils.stringEncode;

public class GeoPointParsingTests  extends ESTestCase {
    static double TOLERANCE = 1E-5;

    public void testGeoPointReset() throws IOException {
        double lat = 1 + randomDouble() * 89;
        double lon = 1 + randomDouble() * 179;

        GeoPoint point = new GeoPoint(0, 0);
        GeoPoint point2 = new GeoPoint(0, 0);
        assertPointsEqual(point, point2);

        assertPointsEqual(point.reset(lat, lon), point2.reset(lat, lon));
        assertPointsEqual(point.reset(0, 0), point2.reset(0, 0));
        assertPointsEqual(point.resetLat(lat), point2.reset(lat, 0));
        assertPointsEqual(point.resetLat(0), point2.reset(0, 0));
        assertPointsEqual(point.resetLon(lon), point2.reset(0, lon));
        assertPointsEqual(point.resetLon(0), point2.reset(0, 0));
        assertCloseTo(point.resetFromGeoHash(stringEncode(lon, lat)), lat, lon);
        assertPointsEqual(point.reset(0, 0), point2.reset(0, 0));
        assertPointsEqual(point.resetFromString(Double.toString(lat) + ", " + Double.toHexString(lon)), point2.reset(lat, lon));
        assertPointsEqual(point.reset(0, 0), point2.reset(0, 0));
    }

    public void testEqualsHashCodeContract() {
        // generate a random geopoint
        final GeoPoint x = RandomGeoGenerator.randomPoint(random());
        final GeoPoint y = new GeoPoint(x.lat(), x.lon());
        final GeoPoint z = new GeoPoint(y.lat(), y.lon());
        // GeoPoint doesn't care about coordinate system bounds, this simply validates inequality
        final GeoPoint a = new GeoPoint(x.lat() + randomIntBetween(1, 5), x.lon() + randomIntBetween(1, 5));

        /** equality test */
        // reflexive
        assertTrue(x.equals(x));
        // symmetry
        assertTrue(x.equals(y));
        // transitivity
        assertTrue(y.equals(z));
        assertTrue(x.equals(z));
        // inequality
        assertFalse(x.equals(a));

        /** hashCode test */
        // symmetry
        assertTrue(x.hashCode() == y.hashCode());
        // transitivity
        assertTrue(y.hashCode() == z.hashCode());
        assertTrue(x.hashCode() == z.hashCode());
        // inequality
        assertFalse(x.hashCode() == a.hashCode());
    }

    public void testGeoPointParsing() throws IOException {
        GeoPoint randomPt = RandomGeoGenerator.randomPoint(random());

        GeoPoint point = GeoUtils.parseGeoPoint(objectLatLon(randomPt.lat(), randomPt.lon()));
        assertPointsEqual(point, randomPt);

        GeoUtils.parseGeoPoint(arrayLatLon(randomPt.lat(), randomPt.lon()), point);
        assertPointsEqual(point, randomPt);

        GeoUtils.parseGeoPoint(geohash(randomPt.lat(), randomPt.lon()), point);
        assertCloseTo(point, randomPt.lat(), randomPt.lon());

        GeoUtils.parseGeoPoint(stringLatLon(randomPt.lat(), randomPt.lon()), point);
        assertCloseTo(point, randomPt.lat(), randomPt.lon());
    }

    // Based on #5390
    public void testInvalidPointEmbeddedObject() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.startObject("location");
        content.field("lat", 0).field("lon", 0);
        content.endObject();
        content.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();

        try {
            GeoUtils.parseGeoPoint(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("field must be either [lat], [lon] or [geohash]"));
        }
    }

    public void testInvalidPointLatHashMix() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lat", 0).field("geohash", stringEncode(0d, 0d));
        content.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();

        try {
            GeoUtils.parseGeoPoint(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("field must be either lat/lon or geohash"));
        }
    }

    public void testInvalidPointLonHashMix() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lon", 0).field("geohash", stringEncode(0d, 0d));
        content.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();

        try {
            GeoUtils.parseGeoPoint(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("field must be either lat/lon or geohash"));
        }
    }

    public void testInvalidField() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lon", 0).field("lat", 0).field("test", 0);
        content.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();

        try {
            GeoUtils.parseGeoPoint(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("field must be either [lat], [lon] or [geohash]"));
        }
    }

    private static XContentParser objectLatLon(double lat, double lon) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lat", lat).field("lon", lon);
        content.endObject();
        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();
        return parser;
    }

    private static XContentParser arrayLatLon(double lat, double lon) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startArray().value(lon).value(lat).endArray();
        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();
        return parser;
    }

    private static XContentParser stringLatLon(double lat, double lon) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.value(Double.toString(lat) + ", " + Double.toString(lon));
        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();
        return parser;
    }

    private static XContentParser geohash(double lat, double lon) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.value(stringEncode(lon, lat));
        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();
        return parser;
    }

    public static void assertPointsEqual(final GeoPoint point1, final GeoPoint point2) {
        assertEquals(point1, point2);
        assertEquals(point1.hashCode(), point2.hashCode());
    }

    public static void assertCloseTo(final GeoPoint point, final double lat, final double lon) {
        assertEquals(point.lat(), lat, TOLERANCE);
        assertEquals(point.lon(), lon, TOLERANCE);
    }
}
