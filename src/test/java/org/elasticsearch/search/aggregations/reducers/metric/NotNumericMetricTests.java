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
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.reducers.metric.InternalMetric;
import org.elasticsearch.search.reducers.metric.MetricsBuilder;
import org.elasticsearch.search.reducers.metric.format.ArrayResult;
import org.elasticsearch.search.reducers.metric.linefit.LineFitResult;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.reducers.ReducerBuilders.arrayReducer;
import static org.elasticsearch.search.reducers.ReducerBuilders.lineFitReducer;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.instanceOf;

public class NotNumericMetricTests extends ElasticsearchIntegrationTest {

    public static String REDUCER_NAME = "metric_name";

    @Test
    public void testVeryBasicArray() throws IOException, ExecutionException, InterruptedException {
        indexData();
        InternalMetric metric = getAndSanityCheckMetric(arrayReducer(REDUCER_NAME));
        double[] expectedArray = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
        assertArrayEquals(expectedArray, ((ArrayResult) (metric.getMetricResult())).getArray(), 0.0);
    }

    @Test
    public void testVeryBasicLineFit() throws IOException, ExecutionException, InterruptedException {
        indexData();
        InternalMetric metric = getAndSanityCheckMetric(lineFitReducer(REDUCER_NAME));
        assertEquals(2.0, ((LineFitResult) metric.getMetricResult()).getSlope(), 1.e-8);
        assertEquals(2.0, ((LineFitResult) metric.getMetricResult()).getBias(), 1.e-8);
        metric = getAndSanityCheckMetric(lineFitReducer(REDUCER_NAME).outputFittedLine(true));
        assertEquals(2.0, ((LineFitResult) metric.getMetricResult()).getSlope(), 1.e-8);
        assertEquals(2.0, ((LineFitResult) metric.getMetricResult()).getBias(), 1.e-8);
        double[] expectedModelValues = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
        assertArrayEquals(((LineFitResult) metric.getMetricResult()).getModelValues(), expectedModelValues, 1.e-8);
    }

    private InternalMetric getAndSanityCheckMetric(MetricsBuilder builder) throws IOException {
        SearchResponse searchResponse = client().prepareSearch("index")
                .addAggregation(histogram("histo").field("hist_field").interval(1))
                .addReducer(builder.bucketsPath("histo").field("_count")).get();
        assertSearchResponse(searchResponse);
        Aggregations reductions = searchResponse.getReductions();
        Aggregation sumReduc = reductions.getAsMap().get(REDUCER_NAME);
        assertNotNull(sumReduc);
        assertThat(sumReduc, instanceOf(InternalMetric.class));
        return (InternalMetric) sumReduc;
    }

    private void indexData() throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < i + 1; j++) {
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
        }
        indexRandom(true, true, indexRequests);
    }
}
