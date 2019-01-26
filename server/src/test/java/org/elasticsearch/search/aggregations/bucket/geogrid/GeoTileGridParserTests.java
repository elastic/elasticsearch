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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.geo.GeoTileUtils;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GeoTileGridParserTests extends ESTestCase {
    public void testParseValidFromInts() throws Exception {
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"precision\":" + precision + ", \"size\": 500, \"shard_size\": 550}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoTileGridAggregationBuilder.parse("geotile_grid", stParser));
    }

    public void testParseValidFromStrings() throws Exception {
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"precision\":\"" + precision + "\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoTileGridAggregationBuilder.parse("geotile_grid", stParser));
    }

    public void testParseDistanceUnitPrecision() throws Exception {
        double distance = randomDoubleBetween(10.0, 100.00, true);
        DistanceUnit unit = randomFrom(DistanceUnit.values());
        if (unit.equals(DistanceUnit.MILLIMETERS)) {
            distance = 5600 + randomDouble(); // 5.6cm is approx. smallest distance
        }
        String distanceString = distance + unit.toString();
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"precision\": \"" + distanceString + "\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        GeoGridAggregationBuilder builder = GeoTileGridAggregationBuilder.parse("geotile_grid", stParser);
        assertNotNull(builder);
        assertThat(builder.precision(), greaterThanOrEqualTo(0));
        assertThat(builder.precision(), lessThanOrEqualTo(29));
    }

    public void testParseInvalidUnitPrecision() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"precision\": \"10kg\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException ex = expectThrows(XContentParseException.class,
                () -> GeoTileGridAggregationBuilder.parse("geotile_grid", stParser));
        assertThat(ex.getMessage(), containsString("[geotile_grid] failed to parse field [precision]"));
        assertThat(ex.getCause(), instanceOf(NumberFormatException.class));
        assertEquals("For input string: \"10kg\"", ex.getCause().getMessage());
    }

    public void testParseDistanceUnitPrecisionTooSmall() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"precision\": \"1cm\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException ex = expectThrows(XContentParseException.class,
                () -> GeoTileGridAggregationBuilder.parse("geotile_grid", stParser));
        assertThat(ex.getMessage(), containsString("[geotile_grid] failed to parse field [precision]"));
        assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
        assertEquals("precision too high [1cm]", ex.getCause().getMessage());
    }

    public void testParseErrorOnBooleanPrecision() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\":false}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException e = expectThrows(XContentParseException.class,
                () -> GeoTileGridAggregationBuilder.parse("geotile_grid", stParser));
        assertThat(ExceptionsHelper.detailedMessage(e),
                containsString("[geotile_grid] precision doesn't support values of type: VALUE_BOOLEAN"));
    }

    public void testParseErrorOnPrecisionOutOfRange() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\":\"30\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        try {
            GeoTileGridAggregationBuilder.parse("geotile_grid", stParser);
            fail();
        } catch (XContentParseException ex) {
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals("Invalid geotile_grid precision of 30. Must be between 0 and 29.", ex.getCause().getMessage());
        }
    }
}
