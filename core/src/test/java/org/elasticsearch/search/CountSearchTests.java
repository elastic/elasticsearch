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

package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

/**
 * {@link SearchType#COUNT} is deprecated but let's make sure it still works as expected.
 */
public class CountSearchTests extends ElasticsearchIntegrationTest {

    public void testDuelCountQueryThenFetch() throws Exception {
        createIndex("idx");
        ensureYellow();
        indexRandom(true,
                client().prepareIndex("idx", "type", "1").setSource("foo", "bar", "bar", 3),
                client().prepareIndex("idx", "type", "2").setSource("foo", "baz", "bar", 10),
                client().prepareIndex("idx", "type", "3").setSource("foo", "foo", "bar", 7));

        final SearchResponse resp1 = client().prepareSearch("idx").setSize(0).addAggregation(AggregationBuilders.sum("bar").field("bar")).execute().get();
        assertSearchResponse(resp1);
        final SearchResponse resp2 = client().prepareSearch("idx").setSearchType(SearchType.COUNT).addAggregation(AggregationBuilders.sum("bar").field("bar")).execute().get();
        assertSearchResponse(resp2);

        assertEquals(resp1.getHits().getTotalHits(), resp2.getHits().getTotalHits());
        Sum sum1 = resp1.getAggregations().get("bar");
        Sum sum2 = resp2.getAggregations().get("bar");
        assertEquals(sum1.getValue(), sum2.getValue(), 0d);
    }

    public void testCloseContextEvenWithExplicitSize() throws Exception {
        createIndex("idx");
        ensureYellow();
        indexRandom(true,
                client().prepareIndex("idx", "type", "1").setSource("foo", "bar", "bar", 3),
                client().prepareIndex("idx", "type", "2").setSource("foo", "baz", "bar", 10),
                client().prepareIndex("idx", "type", "3").setSource("foo", "foo", "bar", 7));

        client().prepareSearch("idx").setSearchType(SearchType.COUNT).setSize(2).addAggregation(AggregationBuilders.sum("bar").field("bar")).execute().get();
    }

}
