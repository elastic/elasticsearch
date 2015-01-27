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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import java.io.IOException;

public class AggregationCollectorTests extends ElasticsearchSingleNodeTest {

    public void testNeedsScores() throws Exception {
        IndexService index = createIndex("idx");
        client().prepareIndex("idx", "type", "1").setSource("f", 5).execute().get();
        client().admin().indices().prepareRefresh("idx").get();
        
        // simple field aggregation, no scores needed
        String fieldAgg = "{ \"my_terms\": {\"terms\": {\"field\": \"f\"}}}";
        assertFalse(needsScores(index, fieldAgg));

        // agg on a script => scores are needed
        String scriptAgg = "{ \"my_terms\": {\"terms\": {\"script\": \"doc['f'].value\"}}}";
        assertTrue(needsScores(index, scriptAgg));

        // make sure the information is propagated to sub aggregations
        String subFieldAgg = "{ \"my_outer_terms\": { \"terms\": { \"field\": \"f\" }, \"aggs\": " + fieldAgg + "}}";
        assertFalse(needsScores(index, subFieldAgg));

        String subScriptAgg = "{ \"my_outer_terms\": { \"terms\": { \"field\": \"f\" }, \"aggs\": " + scriptAgg + "}}";
        assertTrue(needsScores(index, subScriptAgg));

        // top_hits is a particular example of an aggregation that needs scores
        String topHitsAgg = "{ \"my_hits\": {\"top_hits\": {}}}";
        assertTrue(needsScores(index, topHitsAgg));
    }

    private boolean needsScores(IndexService index, String agg) throws IOException {
        AggregatorParsers parser = getInstanceFromNode(AggregatorParsers.class);
        XContentParser aggParser = JsonXContent.jsonXContent.createParser(agg);
        aggParser.nextToken();
        SearchContext searchContext = createSearchContext(index);
        final AggregatorFactories factories = parser.parseAggregators(aggParser, searchContext);
        AggregationContext aggregationContext = new AggregationContext(searchContext);
        final Aggregator[] aggregators = factories.createTopLevelAggregators(aggregationContext);
        assertEquals(1, aggregators.length);
        return aggregators[0].needsScores();
    }

}
