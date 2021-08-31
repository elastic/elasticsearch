/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class GeoTileGridParserTests extends ESTestCase {
    public void testParseValidFromInts() throws Exception {
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        XContentParser stParser = createParser(
            JsonXContent.jsonXContent,
            "{\"field\":\"my_loc\", \"precision\":" + precision + ", \"size\": 500, \"shard_size\": 550}"
        );
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoTileGridAggregationBuilder.PARSER.parse(stParser, "geotile_grid"));
    }

    public void testParseValidFromStrings() throws Exception {
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        XContentParser stParser = createParser(
            JsonXContent.jsonXContent,
            "{\"field\":\"my_loc\", \"precision\":\"" + precision + "\", \"size\": \"500\", \"shard_size\": \"550\"}"
        );
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoTileGridAggregationBuilder.PARSER.parse(stParser, "geotile_grid"));
    }

    public void testParseErrorOnBooleanPrecision() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\":false}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> GeoTileGridAggregationBuilder.PARSER.parse(stParser, "geotile_grid")
        );
        assertThat(
            ExceptionsHelper.detailedMessage(e),
            containsString("[geotile_grid] precision doesn't support values of type: VALUE_BOOLEAN")
        );
    }

    public void testParseErrorOnPrecisionOutOfRange() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\":\"30\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        try {
            GeoTileGridAggregationBuilder.PARSER.parse(stParser, "geotile_grid");
            fail();
        } catch (XContentParseException ex) {
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals("Invalid geotile_grid precision of 30. Must be between 0 and 29.", ex.getCause().getMessage());
        }
    }

    public void testParseValidBounds() throws Exception {
        Rectangle bbox = GeometryTestUtils.randomRectangle();
        XContentParser stParser = createParser(
            JsonXContent.jsonXContent,
            "{\"field\":\"my_loc\", \"precision\": 5, \"size\": 500, \"shard_size\": 550,"
                + "\"bounds\": { "
                + "\"top\": "
                + bbox.getMaxY()
                + ","
                + "\"bottom\": "
                + bbox.getMinY()
                + ","
                + "\"left\": "
                + bbox.getMinX()
                + ","
                + "\"right\": "
                + bbox.getMaxX()
                + "}"
                + "}"
        );
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoTileGridAggregationBuilder.PARSER.parse(stParser, "geotile_grid"));
    }
}
