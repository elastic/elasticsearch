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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;

public class GeoHashGridParserTests extends ESTestCase {
    public void testParseValidFromInts() throws Exception {
        int precision = randomIntBetween(1, 12);
        XContentParser stParser = createParser(JsonXContent.jsonXContent, 
                "{\"field\":\"my_loc\", \"precision\":" + precision + ", \"size\": 500, \"shard_size\": 550}");
        QueryParseContext parseContext = new QueryParseContext(stParser);
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoGridAggregationBuilder.parse("geohash_grid", parseContext));
    }

    public void testParseValidFromStrings() throws Exception {
        int precision = randomIntBetween(1, 12);
        XContentParser stParser = createParser(JsonXContent.jsonXContent, 
                "{\"field\":\"my_loc\", \"precision\":\"" + precision + "\", \"size\": \"500\", \"shard_size\": \"550\"}");
        QueryParseContext parseContext = new QueryParseContext(stParser);
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoGridAggregationBuilder.parse("geohash_grid", parseContext));
    }

    public void testParseErrorOnNonIntPrecision() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\":\"2.0\"}");
        QueryParseContext parseContext = new QueryParseContext(stParser);
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        try {
            GeoGridAggregationBuilder.parse("geohash_grid", parseContext);
            fail();
        } catch (ParsingException ex) {
            assertThat(ex.getCause(), instanceOf(NumberFormatException.class));
            assertEquals("For input string: \"2.0\"", ex.getCause().getMessage());
        }
    }

    public void testParseErrorOnBooleanPrecision() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\":false}");
        QueryParseContext parseContext = new QueryParseContext(stParser);
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        try {
            GeoGridAggregationBuilder.parse("geohash_grid", parseContext);
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("[geohash_grid] precision doesn't support values of type: VALUE_BOOLEAN", ex.getMessage());
        }
    }

    public void testParseErrorOnPrecisionOutOfRange() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\":\"13\"}");
        QueryParseContext parseContext = new QueryParseContext(stParser);
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        try {
            GeoGridAggregationBuilder.parse("geohash_grid", parseContext);
            fail();
        } catch (ParsingException ex) {
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals("Invalid geohash aggregation precision of 13. Must be between 1 and 12.", ex.getCause().getMessage());
        }
    }
}