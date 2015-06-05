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
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;


public class GeoPointParsingTests  extends ElasticsearchTestCase {

    // mind geohash precision and error
    private static final double ERROR = 0.00001d;

    @Test
    public void testGeoPointReset() throws IOException {
        double lat = 1 + randomDouble() * 89;
        double lon = 1 + randomDouble() * 179;

        GeoPoint point = new GeoPoint(0, 0);
        assertCloseTo(point, 0, 0);

        assertCloseTo(point.reset(lat, lon), lat, lon);
        assertCloseTo(point.reset(0, 0), 0, 0);
        assertCloseTo(point.resetLat(lat), lat, 0);
        assertCloseTo(point.resetLat(0), 0, 0);
        assertCloseTo(point.resetLon(lon), 0, lon);
        assertCloseTo(point.resetLon(0), 0, 0);
        assertCloseTo(point.resetFromGeoHash(GeoHashUtils.encode(lat, lon)), lat, lon);
        assertCloseTo(point.reset(0, 0), 0, 0);
        assertCloseTo(point.resetFromString(Double.toString(lat) + ", " + Double.toHexString(lon)), lat, lon);
        assertCloseTo(point.reset(0, 0), 0, 0);
    }
    
    @Test
    public void testGeoPointParsing() throws IOException {
        double lat = randomDouble() * 180 - 90;
        double lon = randomDouble() * 360 - 180;
        
        GeoPoint point = GeoUtils.parseGeoPoint(objectLatLon(lat, lon));
        assertCloseTo(point, lat, lon);
        
        GeoUtils.parseGeoPoint(arrayLatLon(lat, lon), point);
        assertCloseTo(point, lat, lon);

        GeoUtils.parseGeoPoint(geohash(lat, lon), point);
        assertCloseTo(point, lat, lon);

        GeoUtils.parseGeoPoint(stringLatLon(lat, lon), point);
        assertCloseTo(point, lat, lon);
    }

    // Based on issue5390
    @Test(expected = ElasticsearchParseException.class)
    public void testInvalidPointEmbeddedObject() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.startObject("location");
        content.field("lat", 0).field("lon", 0);
        content.endObject();
        content.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();

        GeoUtils.parseGeoPoint(parser);
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testInvalidPointLatHashMix() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lat", 0).field("geohash", GeoHashUtils.encode(0, 0));
        content.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();

        GeoUtils.parseGeoPoint(parser);
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testInvalidPointLonHashMix() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lon", 0).field("geohash", GeoHashUtils.encode(0, 0));
        content.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();

        GeoUtils.parseGeoPoint(parser);
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testInvalidField() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field("lon", 0).field("lat", 0).field("test", 0);
        content.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();

        GeoUtils.parseGeoPoint(parser);
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
        content.value(GeoHashUtils.encode(lat, lon));
        XContentParser parser = JsonXContent.jsonXContent.createParser(content.bytes());
        parser.nextToken();
        return parser;
    }
    
    public static void assertCloseTo(GeoPoint point, double lat, double lon) {
        assertThat(point.lat(), closeTo(lat, ERROR));
        assertThat(point.lon(), closeTo(lon, ERROR));
    }

}
