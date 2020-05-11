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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;


/**
 * Tests for {@link GeoBoundingBox}
 */
public class GeoBoundingBoxTests extends ESTestCase {

    public void testInvalidParseInvalidWKT() throws IOException {
        XContentBuilder bboxBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field("wkt", "invalid")
            .endObject();
        XContentParser parser = createParser(bboxBuilder);
        parser.nextToken();
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> GeoBoundingBox.parseBoundingBox(parser));
        assertThat(e.getMessage(), equalTo("failed to parse WKT bounding box"));
    }

    public void testInvalidParsePoint() throws IOException {
        XContentBuilder bboxBuilder = XContentFactory.jsonBuilder()
            .startObject()
                .field("wkt", "POINT (100.0 100.0)")
            .endObject();
        XContentParser parser = createParser(bboxBuilder);
        parser.nextToken();
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> GeoBoundingBox.parseBoundingBox(parser));
        assertThat(e.getMessage(), equalTo("failed to parse WKT bounding box. [POINT] found. expected [ENVELOPE]"));
    }

    public void testWKT() throws IOException {
        GeoBoundingBox geoBoundingBox = randomBBox();
        assertBBox(geoBoundingBox,
            XContentFactory.jsonBuilder()
                .startObject()
                .field("wkt", geoBoundingBox.toString())
                .endObject()
        );
    }

    public void testTopBottomLeftRight() throws Exception {
        GeoBoundingBox geoBoundingBox = randomBBox();
        assertBBox(geoBoundingBox,
            XContentFactory.jsonBuilder()
                .startObject()
                .field("top", geoBoundingBox.top())
                .field("bottom", geoBoundingBox.bottom())
                .field("left", geoBoundingBox.left())
                .field("right", geoBoundingBox.right())
                .endObject()
        );
    }

    public void testTopLeftBottomRight() throws Exception {
        GeoBoundingBox geoBoundingBox = randomBBox();
        assertBBox(geoBoundingBox,
            XContentFactory.jsonBuilder()
                .startObject()
                .field("top_left", geoBoundingBox.topLeft())
                .field("bottom_right", geoBoundingBox.bottomRight())
                .endObject()
        );
    }

    public void testTopRightBottomLeft() throws Exception {
        GeoBoundingBox geoBoundingBox = randomBBox();
        assertBBox(geoBoundingBox,
            XContentFactory.jsonBuilder()
                .startObject()
                .field("top_right", new GeoPoint(geoBoundingBox.top(), geoBoundingBox.right()))
                .field("bottom_left", new GeoPoint(geoBoundingBox.bottom(), geoBoundingBox.left()))
                .endObject()
        );
    }

    // test that no exception is thrown. BBOX parsing is not validated
    public void testNullTopBottomLeftRight() throws Exception {
        GeoBoundingBox geoBoundingBox = randomBBox();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        for (String field : randomSubsetOf(List.of("top", "bottom", "left", "right"))) {
            switch (field) {
                case "top":
                    builder.field("top", geoBoundingBox.top());
                    break;
                case "bottom":
                    builder.field("bottom", geoBoundingBox.bottom());
                    break;
                case "left":
                    builder.field("left", geoBoundingBox.left());
                    break;
                case "right":
                    builder.field("right", geoBoundingBox.right());
                    break;
                default:
                    throw new IllegalStateException("unexpected branching");
            }
        }
        builder.endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            GeoBoundingBox.parseBoundingBox(parser);
        }
    }

    public void testPointInBounds() {
        for (int iter = 0; iter < 1000; iter++) {
            GeoBoundingBox geoBoundingBox = randomBBox();
            GeoBoundingBox bbox = new GeoBoundingBox(
                new GeoPoint(quantizeLat(geoBoundingBox.top()), quantizeLon(geoBoundingBox.left())),
                    new GeoPoint(quantizeLat(geoBoundingBox.bottom()), quantizeLon(geoBoundingBox.right())));
            if (bbox.left() > bbox.right()) {
                double lonWithin = randomBoolean() ?
                    randomDoubleBetween(bbox.left(), 180.0, true)
                    : randomDoubleBetween(-180.0, bbox.right(), true);
                double latWithin = randomDoubleBetween(bbox.bottom(), bbox.top(), true);
                double lonOutside = randomDoubleBetween(bbox.left(), bbox.right(), true);
                double latOutside = randomBoolean() ? randomDoubleBetween(Math.max(bbox.top(), bbox.bottom()), 90, false)
                    : randomDoubleBetween(-90, Math.min(bbox.bottom(), bbox.top()), false);

                assertTrue(bbox.pointInBounds(lonWithin, latWithin));
                    assertFalse(bbox.pointInBounds(lonOutside, latOutside));
            } else {
                double lonWithin = randomDoubleBetween(bbox.left(), bbox.right(), true);
                double latWithin = randomDoubleBetween(bbox.bottom(), bbox.top(), true);
                double lonOutside = GeoUtils.normalizeLon(randomDoubleBetween(bbox.right(), 180, false));
                double latOutside = GeoUtils.normalizeLat(randomDoubleBetween(bbox.top(), 90, false));

                assertTrue(bbox.pointInBounds(lonWithin, latWithin));
                assertFalse(bbox.pointInBounds(lonOutside, latOutside));
            }
        }
    }

    private void assertBBox(GeoBoundingBox expected, XContentBuilder builder) throws IOException {
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            assertThat(GeoBoundingBox.parseBoundingBox(parser), equalTo(expected));
        }
    }

    public static GeoBoundingBox randomBBox() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        return new GeoBoundingBox(new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon()));
    }

    private static double quantizeLat(double lat) {
        return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
    }

    private static double quantizeLon(double lon) {
        return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
    }
}
