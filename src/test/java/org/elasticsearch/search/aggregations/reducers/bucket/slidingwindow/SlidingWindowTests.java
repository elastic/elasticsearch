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

package org.elasticsearch.search.aggregations.reducers.bucket.slidingwindow;


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.reducers.bucket.BucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.slidingwindow.InternalSlidingWindow;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.reducers.ReducerBuilders.slidingWindowReducer;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.core.Is.is;

public class SlidingWindowTests extends ElasticsearchIntegrationTest {

    @Test
    public void testBasicSelection() throws IOException, ExecutionException, InterruptedException {
        indexData();
        SearchResponse searchResponse = client().prepareSearch("index").addAggregation(histogram("histo").field("hist_field").interval(10)).addReducer(slidingWindowReducer("two_buckets").path("histo").windowSize(5)).get();
        assertSearchResponse(searchResponse);
        Aggregations reductions = searchResponse.getReductions();
        Aggregation slidingWindow = reductions.getAsMap().get("two_buckets");
        assertNotNull(slidingWindow);
        assertTrue(slidingWindow instanceof InternalSlidingWindow);
        for (BucketReducerAggregation.Selection window : ((InternalSlidingWindow) slidingWindow).getBuckets()) {
            assertThat(window.getBuckets().size(), is(5));
        }
    }

    private void indexData() throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i<100; i++ ) {
            indexRequests.add(client().prepareIndex("index", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("hist_field", i)
                    .endObject()));
        }
        indexRandom(true, true, indexRequests);
    }
}
