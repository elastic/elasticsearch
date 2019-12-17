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
public class BoundingBoxTests extends ESTestCase {

    public void testInvalidParseInvalidWKT() throws IOException {
        XContentBuilder bboxBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field("wkt", "invalid")
            .endObject();
        XContentParser parser = createParser(bboxBuilder);
        parser.nextToken();
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> BoundingBox.parseBoundingBox(parser));
        assertThat(e.getMessage(), equalTo("failed to parse WKT bounding box"));
    }

    public void testInvalidParsePoint() throws IOException {
        XContentBuilder bboxBuilder = XContentFactory.jsonBuilder()
            .startObject()
                .field("wkt", "POINT (100.0 100.0)")
            .endObject();
        XContentParser parser = createParser(bboxBuilder);
        parser.nextToken();
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> BoundingBox.parseBoundingBox(parser));
        assertThat(e.getMessage(), equalTo("failed to parse WKT bounding box. [POINT] found. expected [ENVELOPE]"));
    }

    public void testWKT() throws IOException {
        BoundingBox boundingBox = randomBBox();
        assertBBox(boundingBox,
            XContentFactory.jsonBuilder()
                .startObject()
                .field("wkt", boundingBox.toString())
                .endObject()
        );
    }

    public void testTopBottomLeftRight() throws Exception {
        BoundingBox boundingBox = randomBBox();
        assertBBox(boundingBox,
            XContentFactory.jsonBuilder()
                .startObject()
                .field("top", boundingBox.top())
                .field("bottom", boundingBox.bottom())
                .field("left", boundingBox.left())
                .field("right", boundingBox.right())
                .endObject()
        );
    }

    public void testTopLeftBottomRight() throws Exception {
        BoundingBox boundingBox = randomBBox();
        assertBBox(boundingBox,
            XContentFactory.jsonBuilder()
                .startObject()
                .field("top_left", boundingBox.topLeft())
                .field("bottom_right", boundingBox.bottomRight())
                .endObject()
        );
    }

    public void testTopRightBottomLeft() throws Exception {
        BoundingBox boundingBox = randomBBox();
        assertBBox(boundingBox,
            XContentFactory.jsonBuilder()
                .startObject()
                .field("top_right", new GeoPoint(boundingBox.top(), boundingBox.right()))
                .field("bottom_left", new GeoPoint(boundingBox.bottom(), boundingBox.left()))
                .endObject()
        );
    }

    // test that no exception is thrown. BBOX parsing is not validated
    public void testNullTopBottomLeftRight() throws Exception {
        BoundingBox boundingBox = randomBBox();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        for (String field : randomSubsetOf(List.of("top", "bottom", "left", "right"))) {
            switch (field) {
                case "top":
                    builder.field("top", boundingBox.top());
                    break;
                case "bottom":
                    builder.field("bottom", boundingBox.bottom());
                    break;
                case "left":
                    builder.field("left", boundingBox.left());
                    break;
                case "right":
                    builder.field("right", boundingBox.right());
                    break;
                default:
                    throw new IllegalStateException("unexpected branching");
            }
        }
        builder.endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            BoundingBox.parseBoundingBox(parser);
        }
    }

    private void assertBBox(BoundingBox expected, XContentBuilder builder) throws IOException {
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            assertThat(BoundingBox.parseBoundingBox(parser), equalTo(expected));
        }
    }

    private BoundingBox randomBBox() {
        double topLat = GeometryTestUtils.randomLat();
        double bottomLat = randomDoubleBetween(GeoUtils.MIN_LAT, topLat, true);
        return new BoundingBox(new GeoPoint(topLat, GeometryTestUtils.randomLon()),
            new GeoPoint(bottomLat, GeometryTestUtils.randomLon()));
    }
}
