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

package org.elasticsearch.search.aggregations.metrics.geoheatmap;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.aggregations.metrics.geoheatmap.GeoHeatmapAggregationBuilder;
import org.elasticsearch.test.ESTestCase;

/**
 * Tests the construction of the aggregator from JSON
 */
public class GeoHeatmapParserTests extends ESTestCase {

    /**
     * Verifies that integers can be used in the grid_level and max_cells
     * parameters
     */
    public void testParseValidFromInts() throws Exception {
        int gridLevel = randomIntBetween(1, 12);
        int maxCells = randomIntBetween(1, 100_000);
        XContentParser stParser = JsonXContent.jsonXContent.createParser("{\"field\":\"my_loc\", \"grid_level\":" + gridLevel
                + ", \"max_cells\":" + maxCells + ", \"geom\":{\"geo_shape\": {" + "    \"location\": {" + "       \"shape\": {"
                + "       \"type\": \"envelope\"," + "        \"coordinates\" : [[13.0, 53.0], [14.0, 52.0]]" + "     },"
                + "      \"relation\": \"within\"" + "   }" + "}}}");
        QueryParser<GeoShapeQueryBuilder> parser = GeoShapeQueryBuilder::fromXContent;
        IndicesQueriesRegistry mockRegistry = new IndicesQueriesRegistry();
        mockRegistry.register(parser, "geo_shape");
        QueryParseContext parseContext = new QueryParseContext(mockRegistry, stParser, ParseFieldMatcher.STRICT);
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        GeoHeatmapAggregationBuilder builder = GeoHeatmapAggregationBuilder.parse("geo_heatmap", parseContext);
        // can create a factory
        assertNotNull(builder);
    }

    /**
     * Verifies that integer strings can be used in the grid_level and max_cells
     * parameters
     */
    public void testParseValidFromStrings() throws Exception {
        int gridLevel = randomIntBetween(1, 12);
        int maxCells = randomIntBetween(1, 100_000);
        XContentParser stParser = JsonXContent.jsonXContent.createParser("{\"field\":\"my_loc\", \"grid_level\":\"" + gridLevel
                + "\", \"max_cells\": \"" + maxCells + "\"" + ", \"geom\":{\"geo_shape\": {" + "    \"location\": {" + "       \"shape\": {"
                + "       \"type\": \"envelope\"," + "        \"coordinates\" : [[13.0, 53.0], [14.0, 52.0]]" + "     },"
                + "      \"relation\": \"within\"" + "   }" + "}}}");
        QueryParser<GeoShapeQueryBuilder> parser = GeoShapeQueryBuilder::fromXContent;
        IndicesQueriesRegistry mockRegistry = new IndicesQueriesRegistry();
        mockRegistry.register(parser, "geo_shape");
        QueryParseContext parseContext = new QueryParseContext(mockRegistry, stParser, ParseFieldMatcher.STRICT);
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        GeoHeatmapAggregationBuilder builder = GeoHeatmapAggregationBuilder.parse("geo_heatmap", parseContext);
        // can create a factory
        assertNotNull(builder);
    }

}
