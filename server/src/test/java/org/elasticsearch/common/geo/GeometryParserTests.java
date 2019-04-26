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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.test.ESTestCase;

/**
 * Tests for {@link GeometryParser}
 */
public class GeometryParserTests extends ESTestCase {

    public void testGeoJsonParsing() throws Exception {

        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "Point")
            .startArray("coordinates").value(100.0).value(0.0).endArray()
            .endObject();

        try (XContentParser parser = createParser(pointGeoJson)) {
            parser.nextToken();
            assertEquals(new Point(0, 100), GeometryParser.parse(parser, true, randomBoolean(), randomBoolean()));
        }

        XContentBuilder pointGeoJsonWithZ = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "Point")
            .startArray("coordinates").value(100.0).value(0.0).value(10.0).endArray()
            .endObject();

        try (XContentParser parser = createParser(pointGeoJsonWithZ)) {
            parser.nextToken();
            assertEquals(new Point(0, 100, 10.0), GeometryParser.parse(parser, true, randomBoolean(), true));
        }


        try (XContentParser parser = createParser(pointGeoJsonWithZ)) {
            parser.nextToken();
            expectThrows(XContentParseException.class, () -> GeometryParser.parse(parser, true, randomBoolean(), false));
        }

        XContentBuilder polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .startArray("coordinates")
                        .startArray()
                            .startArray().value(100.0).value(1.0).endArray()
                            .startArray().value(101.0).value(1.0).endArray()
                            .startArray().value(101.0).value(0.0).endArray()
                            .startArray().value(100.0).value(0.0).endArray()
                        .endArray()
                    .endArray()
                .endObject();

        Polygon p = new Polygon(new LinearRing(new double[] {1d, 1d, 0d, 0d, 1d}, new double[] {100d, 101d, 101d, 100d, 100d}));
        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            // Coerce should automatically close the polygon
            assertEquals(p, GeometryParser.parse(parser, true, true, randomBoolean()));
        }

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            // No coerce - the polygon parsing should fail
            expectThrows(XContentParseException.class, () -> GeometryParser.parse(parser, true, false, randomBoolean()));
        }
    }

    public void testWKTParsing() throws Exception {
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", "Point (100 0)")
            .endObject();

        try (XContentParser parser = createParser(pointGeoJson)) {
            parser.nextToken(); // Start object
            parser.nextToken(); // Field Name
            parser.nextToken(); // Field Value
            assertEquals(new Point(0, 100), GeometryParser.parse(parser, true, randomBoolean(), randomBoolean()));
        }
    }

    public void testNullParsing() throws Exception {
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .nullField("foo")
            .endObject();

        try (XContentParser parser = createParser(pointGeoJson)) {
            parser.nextToken(); // Start object
            parser.nextToken(); // Field Name
            parser.nextToken(); // Field Value
            assertNull(GeometryParser.parse(parser, true, randomBoolean(), randomBoolean()));
        }
    }

    public void testUnsupportedValueParsing() throws Exception {
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", 42)
            .endObject();

        try (XContentParser parser = createParser(pointGeoJson)) {
            parser.nextToken(); // Start object
            parser.nextToken(); // Field Name
            parser.nextToken(); // Field Value
            ElasticsearchParseException ex = expectThrows(ElasticsearchParseException.class,
                () -> GeometryParser.parse(parser, true, randomBoolean(), randomBoolean()));
            assertEquals("shape must be an object consisting of type and coordinates", ex.getMessage());
        }
    }
}
