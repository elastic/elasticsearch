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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.bucket.GeoHashGridTests;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GeoHashGridParserTests extends ESTestCase {
    public void testParseValidFromInts() throws Exception {
        GeoHashType type = GeoHashGridTests.randomType();
        int precision = GeoHashGridTests.randomPrecision(type);
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"hash_type\":\"" + type + "\", \"precision\":" + precision +
                    ", \"size\": 500, \"shard_size\": 550}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoGridAggregationBuilder.parse("geohash_grid", stParser));
    }

    public void testParseValidFromStrings() throws Exception {
        GeoHashType type = GeoHashGridTests.randomType();
        int precision = GeoHashGridTests.randomPrecision(type);
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"hash_type\":\"" + type + "\", \"precision\":\"" + precision +
                    "\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoGridAggregationBuilder.parse("geohash_grid", stParser));
    }

    public void testParseOpenLocationCode() throws Exception {
        int precision = RandomPicks.randomFrom(random(), GeoPlusCodeHandler.ALLOWED_LENGTHS);
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"field\":\"my_loc\", \"hash_type\":\"pluscode\", \"precision\":\"" + precision +
                "\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoGridAggregationBuilder.parse("geohash_grid", stParser));
    }

    public void testParseDistanceUnitPrecision() throws Exception {
        double distance = randomDoubleBetween(10.0, 100.00, true);
        DistanceUnit unit = randomFrom(DistanceUnit.values());
        if (unit.equals(DistanceUnit.MILLIMETERS)) {
            distance = 5600 + randomDouble(); // 5.6cm is approx. smallest distance represented by precision 12
        }
        String distanceString = distance + unit.toString();
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"precision\": \"" + distanceString + "\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        GeoGridAggregationBuilder builder = GeoGridAggregationBuilder.parse("geohash_grid", stParser);
        assertNotNull(builder);
        assertThat(builder.precision(), greaterThanOrEqualTo(0));
        assertThat(builder.precision(), lessThanOrEqualTo(12));
    }

    public void testParseInvalidUnitPrecision() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"precision\": \"10kg\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException ex = expectThrows(XContentParseException.class,
                () -> GeoGridAggregationBuilder.parse("geohash_grid", stParser));
        assertThat(ex.getMessage(), containsString("failed to build [geohash_grid] after last required field arrived"));

        Throwable cause = ex.getCause();
        assertThat(cause, instanceOf(XContentParseException.class));
        assertThat(cause.getMessage(), containsString("[geohash_grid] failed to parse field [precision]"));

        cause = cause.getCause();
        assertThat(cause, instanceOf(NumberFormatException.class));
        assertThat(cause.getMessage(), containsString("For input string: \"10kg\""));
    }

    public void testParseDistanceUnitPrecisionTooSmall() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"precision\": \"1cm\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException ex = expectThrows(XContentParseException.class,
                () -> GeoGridAggregationBuilder.parse("geohash_grid", stParser));
        assertThat(ex.getMessage(), containsString("failed to build [geohash_grid] after last required field arrived"));

        Throwable cause = ex.getCause();
        assertThat(cause, instanceOf(XContentParseException.class));
        assertThat(cause.getMessage(), containsString("[geohash_grid] failed to parse field [precision]"));

        cause = cause.getCause();
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertEquals("precision too high [1cm]", cause.getMessage());
    }

    public void testParseErrorOnBooleanPrecision() throws Exception {
        GeoHashType type = GeoHashGridTests.randomType();
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"field\":\"my_loc\", \"hash_type\":\"" + type + "\", \"precision\":false}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException ex = expectThrows(XContentParseException.class,
                () -> GeoGridAggregationBuilder.parse("geohash_grid", stParser));
        assertThat(ex.getMessage(), containsString("[geohash_grid] failed to parse field [precision]"));

        Throwable cause = ex.getCause();
        assertThat(cause, instanceOf(XContentParseException.class));
        assertThat(cause.getMessage(), containsString("[geohash_grid] failed to parse field [precision]" +
            " in [geohash_grid]. It must be either an integer or a string"));
    }

    public void testParseErrorOnPrecisionOutOfRange() throws Exception {
        final GeoHashType type = GeoHashGridTests.randomType();
        final int precision = GeoHashGridTests.maxPrecision(type) + 1;
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"field\":\"my_loc\", \"hash_type\":\"" + type + "\", \"precision\":\""+ precision +"\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        try {
            GeoGridAggregationBuilder.parse("geohash_grid", stParser);
            fail();
        } catch (XContentParseException ex) {
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            String expectedMsg;
            switch (type) {
                case GEOHASH:
                    expectedMsg = "Invalid geohash aggregation precision of 13. Must be between 1 and 12.";
                    break;
                case PLUSCODE:
                    expectedMsg = "Invalid geohash pluscode aggregation precision of 15. Must be between 4 and 14, " +
                        "and must be even if less than 8.";
                    break;
                default:
                    throw new IllegalArgumentException("GeoHashType." + type.name() + " was not added to the test");
            }
            assertEquals(expectedMsg, ex.getCause().getMessage());
        }
    }
}
