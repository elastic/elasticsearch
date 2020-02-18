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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
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
            GeometryFormat format = new GeometryParser(true, randomBoolean(), randomBoolean()).geometryFormat(parser);
            assertEquals(new Point(100, 0), format.fromXContent(parser));
            XContentBuilder newGeoJson = XContentFactory.jsonBuilder();
            format.toXContent(new Point(100, 10), newGeoJson, ToXContent.EMPTY_PARAMS);
            assertEquals("{\"type\":\"Point\",\"coordinates\":[100.0,10.0]}", Strings.toString(newGeoJson));
        }

        XContentBuilder pointGeoJsonWithZ = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "Point")
            .startArray("coordinates").value(100.0).value(0.0).value(10.0).endArray()
            .endObject();

        try (XContentParser parser = createParser(pointGeoJsonWithZ)) {
            parser.nextToken();
            assertEquals(new Point(100, 0, 10.0), new GeometryParser(true, randomBoolean(), true).parse(parser));
        }


        try (XContentParser parser = createParser(pointGeoJsonWithZ)) {
            parser.nextToken();
            expectThrows(IllegalArgumentException.class, () -> new GeometryParser(true, randomBoolean(), false).parse(parser));
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

        Polygon p = new Polygon(new LinearRing(new double[]{100d, 101d, 101d, 100d, 100d}, new double[]{1d, 1d, 0d, 0d, 1d}));
        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            // Coerce should automatically close the polygon
            assertEquals(p, new GeometryParser(true, true, randomBoolean()).parse(parser));
        }

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            // No coerce - the polygon parsing should fail
            expectThrows(XContentParseException.class, () -> new GeometryParser(true, false, randomBoolean()).parse(parser));
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
            GeometryFormat format = new GeometryParser(true, randomBoolean(), randomBoolean()).geometryFormat(parser);
            assertEquals(new Point(100, 0), format.fromXContent(parser));
            XContentBuilder newGeoJson = XContentFactory.jsonBuilder().startObject().field("val");
            format.toXContent(new Point(100, 10), newGeoJson, ToXContent.EMPTY_PARAMS);
            newGeoJson.endObject();
            assertEquals("{\"val\":\"POINT (100.0 10.0)\"}", Strings.toString(newGeoJson));
        }

        // Make sure we can parse values outside the normal lat lon boundaries
        XContentBuilder lineGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", "LINESTRING (100 0, 200 10)")
            .endObject();

        try (XContentParser parser = createParser(lineGeoJson)) {
            parser.nextToken(); // Start object
            parser.nextToken(); // Field Name
            parser.nextToken(); // Field Value
            assertEquals(new Line(new double[]{100, 200}, new double[]{0, 10}),
                new GeometryParser(true, randomBoolean(), randomBoolean()).parse(parser));
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
            GeometryFormat format = new GeometryParser(true, randomBoolean(), randomBoolean()).geometryFormat(parser);
            assertNull(format.fromXContent(parser));

            XContentBuilder newGeoJson = XContentFactory.jsonBuilder().startObject().field("val");
            // if we serialize non-null value - it should be serialized as geojson
            format.toXContent(new Point(100, 10), newGeoJson, ToXContent.EMPTY_PARAMS);
            newGeoJson.endObject();
            assertEquals("{\"val\":{\"type\":\"Point\",\"coordinates\":[100.0,10.0]}}", Strings.toString(newGeoJson));

            newGeoJson = XContentFactory.jsonBuilder().startObject().field("val");
            format.toXContent(null, newGeoJson, ToXContent.EMPTY_PARAMS);
            newGeoJson.endObject();
            assertEquals("{\"val\":null}", Strings.toString(newGeoJson));

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
                () -> new GeometryParser(true, randomBoolean(), randomBoolean()).parse(parser));
            assertEquals("shape must be an object consisting of type and coordinates", ex.getMessage());
        }
    }
}
