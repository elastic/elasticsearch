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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import java.io.IOException;
import java.util.function.DoubleSupplier;

import static org.elasticsearch.geometry.utils.Geohash.stringEncode;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.is;

public class GeoPointParsingTests  extends ESTestCase {
    private static final double TOLERANCE = 1E-5;

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
        assertPointsEqual(point.resetFromString("POINT(" + lon + " " + lat + ")"), point2.reset(lat, lon));
    }

    public void testParseWktInvalid() {
        GeoPoint point = new GeoPoint(0, 0);
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> point.resetFromString("NOT A POINT(1 2)")
        );
        assertEquals("Invalid WKT format", e.getMessage());

        Exception e2 = expectThrows(
            ElasticsearchParseException.class,
            () -> point.resetFromString("MULTIPOINT(1 2, 3 4)")
        );
        assertEquals("[geo_point] supports only POINT among WKT primitives, but found MULTIPOINT", e2.getMessage());
    }

    public void testEqualsHashCodeContract() {
        // GeoPoint doesn't care about coordinate system bounds, this simply validates equality and hashCode.
        final DoubleSupplier randomDelta = () -> randomValueOtherThan(0.0, () -> randomDoubleBetween(-1000000, 1000000, true));
        checkEqualsAndHashCode(RandomGeoGenerator.randomPoint(random()), GeoPoint::new,
            pt -> new GeoPoint(pt.lat() + randomDelta.getAsDouble(), pt.lon()));
        checkEqualsAndHashCode(RandomGeoGenerator.randomPoint(random()), GeoPoint::new,
            pt -> new GeoPoint(pt.lat(), pt.lon() + randomDelta.getAsDouble()));
        checkEqualsAndHashCode(RandomGeoGenerator.randomPoint(random()), GeoPoint::new,
            pt -> new GeoPoint(pt.lat() + randomDelta.getAsDouble(), pt.lon() + randomDelta.getAsDouble()));
    }

    public void testGeoPointParsing() throws IOException {
        GeoPoint randomPt = RandomGeoGenerator.randomPoint(random());

        GeoPoint point = GeoUtils.parseGeoPoint(objectLatLon(randomPt.lat(), randomPt.lon()));
        assertPointsEqual(point, randomPt);

        GeoUtils.parseGeoPoint(toObject(objectLatLon(randomPt.lat(), randomPt.lon())), randomBoolean());
        assertPointsEqual(point, randomPt);

        GeoUtils.parseGeoPoint(arrayLatLon(randomPt.lat(), randomPt.lon()), point);
        assertPointsEqual(point, randomPt);

        GeoUtils.parseGeoPoint(toObject(arrayLatLon(randomPt.lat(), randomPt.lon())), randomBoolean());
        assertPointsEqual(point, randomPt);

        GeoUtils.parseGeoPoint(geohash(randomPt.lat(), randomPt.lon()), point);
        assertCloseTo(point, randomPt.lat(), randomPt.lon());

        GeoUtils.parseGeoPoint(toObject(geohash(randomPt.lat(), randomPt.lon())), randomBoolean());
        assertCloseTo(point, randomPt.lat(), randomPt.lon());

        GeoUtils.parseGeoPoint(stringLatLon(randomPt.lat(), randomPt.lon()), point);
        assertCloseTo(point, randomPt.lat(), randomPt.lon());

        GeoUtils.parseGeoPoint(toObject(stringLatLon(randomPt.lat(), randomPt.lon())), randomBoolean());
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

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content))) {
            parser.nextToken();
            Exception e = expectThrows(ElasticsearchParseException.class, () -> GeoUtils.parseGeoPoint(parser));
            assertThat(e.getMessage(), is("field must be either [lat], [lon] or [geohash]"));
        }
        try (XContentParser parser2 = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content))) {
            parser2.nextToken();
            Exception e = expectThrows(ElasticsearchParseException.class, () ->
                GeoUtils.parseGeoPoint(toObject(parser2), randomBoolean()));
            assertThat(e.getMessage(), is("field must be either [lat], [lon] or [geohash]"));
        }
    }

    public void testInvalidPointLatHashMix() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lat", 0).field("geohash", stringEncode(0d, 0d));
        content.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content))) {
            parser.nextToken();
            Exception e = expectThrows(ElasticsearchParseException.class, () -> GeoUtils.parseGeoPoint(parser));
            assertThat(e.getMessage(), is("field must be either lat/lon or geohash"));
        }
        try (XContentParser parser2 = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content))) {
            parser2.nextToken();
            Exception e = expectThrows(ElasticsearchParseException.class, () ->
                GeoUtils.parseGeoPoint(toObject(parser2), randomBoolean()));
            assertThat(e.getMessage(), is("field must be either lat/lon or geohash"));
        }
    }

    public void testInvalidPointLonHashMix() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lon", 0).field("geohash", stringEncode(0d, 0d));
        content.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content))) {
            parser.nextToken();

            Exception e = expectThrows(ElasticsearchParseException.class, () -> GeoUtils.parseGeoPoint(parser));
            assertThat(e.getMessage(), is("field must be either lat/lon or geohash"));
        }
        try (XContentParser parser2 = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content))) {
            parser2.nextToken();
            Exception e = expectThrows(ElasticsearchParseException.class, () ->
                GeoUtils.parseGeoPoint(toObject(parser2), randomBoolean()));
            assertThat(e.getMessage(), is("field must be either lat/lon or geohash"));
        }
    }

    public void testInvalidField() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lon", 0).field("lat", 0).field("test", 0);
        content.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content))) {
            parser.nextToken();
            Exception e = expectThrows(ElasticsearchParseException.class, () -> GeoUtils.parseGeoPoint(parser));
            assertThat(e.getMessage(), is("field must be either [lat], [lon] or [geohash]"));
        }

        try (XContentParser parser2 = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content))) {
            parser2.nextToken();
            Exception e = expectThrows(ElasticsearchParseException.class, () ->
                GeoUtils.parseGeoPoint(toObject(parser2), randomBoolean()));
            assertThat(e.getMessage(), is("field must be either [lat], [lon] or [geohash]"));
        }
    }

    public void testInvalidGeoHash() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("geohash", "!!!!");
        content.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content))) {
            parser.nextToken();

            Exception e = expectThrows(ElasticsearchParseException.class, () -> GeoUtils.parseGeoPoint(parser));
            assertThat(e.getMessage(), is("unsupported symbol [!] in geohash [!!!!]"));
        }
    }

    private XContentParser objectLatLon(double lat, double lon) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lat", lat).field("lon", lon);
        content.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content));
        parser.nextToken();
        return parser;
    }

    private XContentParser arrayLatLon(double lat, double lon) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startArray().value(lon).value(lat).endArray();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content));
        parser.nextToken();
        return parser;
    }

    private XContentParser stringLatLon(double lat, double lon) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.value(Double.toString(lat) + ", " + Double.toString(lon));
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content));
        parser.nextToken();
        return parser;
    }

    private XContentParser geohash(double lat, double lon) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.value(stringEncode(lon, lat));
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content));
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

    public static Object toObject(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return parser.text();
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            return parser.numberValue();
        } else if (token == XContentParser.Token.START_OBJECT) {
            return parser.map();
        } else if (token == XContentParser.Token.START_ARRAY) {
            return parser.list();
        } else {
            fail("Unexpected token " + token);
        }
        return null;
    }
}
