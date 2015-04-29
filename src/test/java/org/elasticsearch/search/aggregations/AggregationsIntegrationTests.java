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

import com.google.common.collect.Lists;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;


@ElasticsearchIntegrationTest.SuiteScopeTest
public class AggregationsIntegrationTests extends ElasticsearchIntegrationTest {

    static int numDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("index").addMapping("type", "f", "type=string").get());
        ensureYellow("index");
        numDocs = randomIntBetween(1, 20);
        List<IndexRequestBuilder> docs = Lists.newArrayList();
        for (int i = 0; i < numDocs; ++i) {
            docs.add(client().prepareIndex("index", "type").setSource("f", Integer.toString(i / 3)));
        }
        indexRandom(true, docs);
    }

    public void testScan() {
        try {
            client().prepareSearch("index").setSearchType(SearchType.SCAN).setScroll(new TimeValue(500)).addAggregation(terms("f").field("f")).get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertTrue(e.toString(), e.toString().contains("aggregations are not supported with search_type=scan"));
        }
    }

    public void testScroll() {
        final int size = randomIntBetween(1, 4);
        SearchResponse response = client().prepareSearch("index")
                .setSize(size).setScroll(new TimeValue(500))
                .addAggregation(terms("f").field("f")).get();
        assertSearchResponse(response);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Terms terms = aggregations.get("f");
        assertEquals(Math.min(numDocs, 3L), terms.getBucketByKey("0").getDocCount());

        int total = response.getHits().getHits().length;
        while (response.getHits().hits().length > 0) {
            response = client().prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(500))
                    .execute().actionGet();
            assertNull(response.getAggregations());
            total += response.getHits().hits().length;
        }
        clearScroll(response.getScrollId());
        assertEquals(numDocs, total);
    }

}
