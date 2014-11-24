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

package org.elasticsearch.search.aggregations.reducers.metric;


import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.reducers.bucket.BucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.union.InternalUnion;
import org.elasticsearch.search.reducers.metric.avg.AvgReducer;
import org.elasticsearch.search.reducers.metric.sum.InternalSum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.reducers.ReducerBuilders.sumReducer;
import static org.elasticsearch.search.reducers.ReducerBuilders.unionReducer;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;

public class SumTests extends ElasticsearchIntegrationTest {

    @Test
    @LuceneTestCase.AwaitsFix(bugUrl = "results are not streamed correctly, sometimes the objects returned are not reducer...AvgReducer but aggregation...AvgReducer")
    public void testVeryBasicSum() throws IOException, ExecutionException, InterruptedException {
        indexData();
        SearchResponse searchResponse = client().prepareSearch("index")
                .addAggregation(histogram("histo").field("hist_field").interval(10))
                .addReducer(sumReducer("sum_docs").buckets("histo").field("_count")).get();
        assertSearchResponse(searchResponse);
        Aggregations reductions = searchResponse.getReductions();
        Aggregation sumReduc = reductions.getAsMap().get("sum_docs");
        assertNotNull(sumReduc);
        assertTrue(sumReduc instanceof InternalSum);
        assertThat(((InternalSum)sumReduc).getValue(), equalTo(200d));
    }

    @Test
    @LuceneTestCase.AwaitsFix(bugUrl = "need to make work for all sorts of agg trees")
    public void testBasicSum() throws IOException, ExecutionException, InterruptedException {
        indexData();
        SearchResponse searchResponse = client().prepareSearch("index")
                .addAggregation(terms("labels").field("label_field").subAggregation(histogram("histo").field("hist_field").interval(10)))
                .addReducer(sumReducer("sum_docs").buckets("labels.histo").field("_count")).get();
        assertSearchResponse(searchResponse);
        Aggregations reductions = searchResponse.getReductions();
        Aggregation avgReduc = reductions.getAsMap().get("sum_docs");
        assertNotNull(avgReduc);
        assertTrue(avgReduc instanceof AvgReducer);
        for (BucketReducerAggregation.Selection reducerBucket : ((InternalUnion) avgReduc).getBuckets()) {
            assertThat(reducerBucket.getBuckets().size(), is(1));
            assertThat(((MultiBucketsAggregation)((reducerBucket.getBuckets().get(0).getAggregations())).get("labels")).getBuckets().size(), is(2));
        }
    }

    private void indexData() throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i<100; i++ ) {
            indexRequests.add(client().prepareIndex("index", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("hist_field", i)
                    .field("label_field", "label" + Integer.toString(1))
                    .endObject()));
            indexRequests.add(client().prepareIndex("index", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("hist_field", i)
                    .field("label_field", "label" + Integer.toString(2))
                    .endObject()));
        }
        indexRandom(true, true, indexRequests);
    }
}
