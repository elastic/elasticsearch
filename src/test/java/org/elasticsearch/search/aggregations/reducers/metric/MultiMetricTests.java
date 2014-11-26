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


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.reducers.metric.InternalMetric;
import org.elasticsearch.search.reducers.metric.SimpleMetricsBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.reducers.ReducerBuilders.deltaReducer;
import static org.elasticsearch.search.reducers.ReducerBuilders.statsReducer;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class MultiMetricTests extends ElasticsearchIntegrationTest {

    @Test
    public void testVeryBasicDelta() throws IOException, ExecutionException, InterruptedException {
        indexData();
        InternalMetric metric = getAndSanityCheckMetric(deltaReducer("delta_docs"), "delta_docs");
        assertThat(metric.value("delta"), equalTo(0d));
    }

    @Test
    public void testVeryBasicStats() throws IOException, ExecutionException, InterruptedException {
        indexData();
        InternalMetric metric = getAndSanityCheckMetric(statsReducer("stats_docs"), "stats_docs");
        assertThat(metric.value("length"), equalTo(10d));
    }

    private InternalMetric getAndSanityCheckMetric(SimpleMetricsBuilder builder, String reducerName) throws IOException {
        SearchResponse searchResponse = client().prepareSearch("index")
                .addAggregation(histogram("histo").field("hist_field").interval(10))
                .addReducer(builder.bucketsPath("histo").field("_count")).get();
        assertSearchResponse(searchResponse);
        Aggregations reductions = searchResponse.getReductions();
        Aggregation sumReduc = reductions.getAsMap().get(reducerName);
        assertNotNull(sumReduc);
        assertThat(sumReduc, instanceOf(InternalMetric.class));
        return (InternalMetric) sumReduc;
    }

    private InternalMetric getAndSanityCheckMetric(String metricType, String reducerName) throws IOException {
        XContentBuilder jsonRequest = jsonBuilder().startObject()
                .startObject("aggs")
                .startObject("histo")
                .startObject("histogram")
                .field("field", "hist_field")
                .field("interval", 10)
                .endObject()
                .endObject()
                .endObject()
                .startArray("reducers")
                .startObject()
                .startObject(reducerName)
                .startObject(metricType)
                .field("buckets", "histo")
                .field("field", "_count")
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject();
        logger.info("request {}", jsonRequest.string());

        SearchResponse searchResponse = client().prepareSearch("index").setSource(jsonRequest).get();
        assertSearchResponse(searchResponse);
        Aggregations reductions = searchResponse.getReductions();
        Aggregation sumReduc = reductions.getAsMap().get(reducerName);
        assertNotNull(sumReduc);
        assertThat(sumReduc, instanceOf(InternalMetric.class));
        return (InternalMetric) sumReduc;
    }

    private void indexData() throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
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
