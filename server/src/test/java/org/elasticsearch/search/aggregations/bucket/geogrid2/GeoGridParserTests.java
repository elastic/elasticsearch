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
package org.elasticsearch.search.aggregations.bucket.geogrid2;

import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.bucket.GeoGridTests;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GeoGridParserTests extends ESTestCase {
    public void testParseValidFromInts() throws Exception {
        GeoGridType type = GeoGridTests.randomType();
        int precision = GeoGridTests.randomPrecision(type);
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"hash_type\":\"" + type.getName() + "\", \"field\":\"my_loc\", \"precision\":" + precision +
                ", \"size\": 500, \"shard_size\": 550}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoGridAggregationBuilder2.parse("geo_grid", stParser));
    }

    public void testParseValidFromStrings() throws Exception {
        GeoGridType type = GeoGridTests.randomType();
        int precision = GeoGridTests.randomPrecision(type);
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"hash_type\":\"" + type.getName() + "\", \"field\":\"my_loc\", \"precision\":\"" + precision +
                "\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoGridAggregationBuilder2.parse("geo_grid", stParser));
    }

    public void testParseDistanceUnitPrecision() throws Exception {
        double distance = randomDoubleBetween(10.0, 100.00, true);
        DistanceUnit unit = randomFrom(DistanceUnit.values());
        if (unit.equals(DistanceUnit.MILLIMETERS)) {
            distance = 5600 + randomDouble(); // 5.6cm is approx. smallest distance represented by precision 12
        }
        String distanceString = distance + unit.toString();
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"hash_type\":\"geohash\", \"field\":\"my_loc\", \"precision\": \"" + distanceString +
                "\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        GeoGridAggregationBuilder2 builder = GeoGridAggregationBuilder2.parse("geo_grid", stParser);
        assertNotNull(builder);
        assertThat(builder.precision(), greaterThanOrEqualTo(0));
        assertThat(builder.precision(), lessThanOrEqualTo(12));
    }

    public void testParseInvalidUnitPrecision() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"hash_type\":\"geohash\", \"field\":\"my_loc\", \"precision\": \"10kg\", " +
                "\"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException ex = expectThrows(XContentParseException.class,
            () -> GeoGridAggregationBuilder2.parse("geo_grid", stParser));
        assertThat(ex.getMessage(), containsString("[geo_grid] failed to parse field [precision]"));

        Throwable cause = ex.getCause();
        assertThat(cause, instanceOf(NumberFormatException.class));
        assertThat(cause.getMessage(), containsString("For input string: \"10kg\""));
    }

    public void testParseDistanceUnitPrecisionTooSmall() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"hash_type\":\"geohash\", \"field\":\"my_loc\", " +
                "\"precision\": \"1cm\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException ex = expectThrows(XContentParseException.class,
            () -> GeoGridAggregationBuilder2.parse("geo_grid", stParser));
        assertThat(ex.getMessage(), containsString("[geo_grid] failed to parse field [precision]"));

        Throwable cause = ex.getCause();
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertEquals("precision too high [1cm]", cause.getMessage());
    }

    public void testParseErrorOnBooleanPrecision() throws Exception {
        GeoGridType type = GeoGridTests.randomType();
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"hash_type\":\"" + type.getName() + "\", \"field\":\"my_loc\", \"precision\":false}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException ex = expectThrows(XContentParseException.class,
            () -> GeoGridAggregationBuilder2.parse("geo_grid", stParser));
        assertThat(ex.getMessage(), containsString("[geo_grid] failed to parse field [precision]"));

        Throwable cause = ex.getCause();
        assertThat(cause, instanceOf(XContentParseException.class));
        assertThat(cause.getMessage(), containsString("[geo_grid] failed to parse field [precision]" +
            " in [geo_grid]. It must be either an integer or a string"));
    }

    public void testParseErrorOnPrecisionOutOfRange() throws Exception {
        final GeoGridType type = GeoGridTests.randomType();
        final int precision = GeoGridTests.maxPrecision(type) + 1;
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"hash_type\":\"" + type.getName() + "\", \"field\":\"my_loc\", \"precision\":\""+ precision +"\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        try {
            GeoGridAggregationBuilder2.parse("geo_grid", stParser);
            fail();
        } catch (XContentParseException ex) {
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            String expectedMsg;
            if (type.getClass() == GeoHashType.class) {
                expectedMsg = "Invalid geohash aggregation precision of 13. Must be between 1 and 12.";
            } else {
                throw new IllegalArgumentException(type.getClass() + " was not added to the test");
            }
            assertEquals(expectedMsg, ex.getCause().getMessage());
        }
    }
}
