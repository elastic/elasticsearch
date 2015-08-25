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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

public class GeoHashGridParserTests extends ESTestCase {
    public void testParseValidFromInts() throws Exception {
        SearchContext searchContext = new TestSearchContext();
        int precision = randomIntBetween(1, 12);
        XContentParser stParser = JsonXContent.jsonXContent.createParser(
                "{\"field\":\"my_loc\", \"precision\":" + precision + ", \"size\": 500, \"shard_size\": 550}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        GeoHashGridParser parser = new GeoHashGridParser();
        // can create a factory
        assertNotNull(parser.parse("geohash_grid", stParser, searchContext));
    }

    public void testParseValidFromStrings() throws Exception {
        SearchContext searchContext = new TestSearchContext();
        int precision = randomIntBetween(1, 12);
        XContentParser stParser = JsonXContent.jsonXContent.createParser(
                "{\"field\":\"my_loc\", \"precision\":\"" + precision + "\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        GeoHashGridParser parser = new GeoHashGridParser();
        // can create a factory
        assertNotNull(parser.parse("geohash_grid", stParser, searchContext));
    }

    public void testParseErrorOnNonIntPrecision() throws Exception {
        SearchContext searchContext = new TestSearchContext();
        XContentParser stParser = JsonXContent.jsonXContent.createParser("{\"field\":\"my_loc\", \"precision\":\"2.0\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        GeoHashGridParser parser = new GeoHashGridParser();
        try {
            parser.parse("geohash_grid", stParser, searchContext);
            fail();
        } catch (NumberFormatException ex) {
            assertEquals("For input string: \"2.0\"", ex.getMessage());
        }
    }

    public void testParseErrorOnBooleanPrecision() throws Exception {
        SearchContext searchContext = new TestSearchContext();
        XContentParser stParser = JsonXContent.jsonXContent.createParser("{\"field\":\"my_loc\", \"precision\":false}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        GeoHashGridParser parser = new GeoHashGridParser();
        try {
            parser.parse("geohash_grid", stParser, searchContext);
            fail();
        } catch (SearchParseException ex) {
            assertEquals("Unexpected token VALUE_BOOLEAN [precision] in [geohash_grid].", ex.getMessage());
        }
    }

    public void testParseErrorOnPrecisionOutOfRange() throws Exception {
        SearchContext searchContext = new TestSearchContext();
        XContentParser stParser = JsonXContent.jsonXContent.createParser("{\"field\":\"my_loc\", \"precision\":\"13\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        GeoHashGridParser parser = new GeoHashGridParser();
        try {
            parser.parse("geohash_grid", stParser, searchContext);
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Invalid geohash aggregation precision of 13. Must be between 1 and 12.", ex.getMessage());
        }
    }
}