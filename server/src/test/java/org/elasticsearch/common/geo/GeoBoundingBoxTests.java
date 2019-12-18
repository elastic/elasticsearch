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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;


/**
 * Tests for {@code GeoJSONShapeParser}
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

    private void assertBBox(GeoBoundingBox expected, XContentBuilder builder) throws IOException {
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            assertThat(GeoBoundingBox.parseBoundingBox(parser), equalTo(expected));
        }
    }

    private GeoBoundingBox randomBBox() {
        double topLat = GeometryTestUtils.randomLat();
        double bottomLat = randomDoubleBetween(GeoUtils.MIN_LAT, topLat, true);
        return new GeoBoundingBox(new GeoPoint(topLat, GeometryTestUtils.randomLon()),
            new GeoPoint(bottomLat, GeometryTestUtils.randomLon()));
    }
}
