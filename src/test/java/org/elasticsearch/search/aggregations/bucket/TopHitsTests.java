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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.search.aggregations.bucket.tophits.TopHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest()
public class TopHitsTests extends ElasticsearchIntegrationTest {

    private static final String TERMS_AGGS_FIELD = "terms";
    private static final String SORT_FIELD = "sort";

    public static String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            builders.add(client().prepareIndex("idx", "type", Integer.toString(i)).setSource(jsonBuilder()
                    .startObject()
                    .field(TERMS_AGGS_FIELD, "val" + (i / 10))
                    .field(SORT_FIELD, i + 1)
                    .field("text", "some text to entertain")
                    .endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    private String key(Terms.Bucket bucket) {
        return randomBoolean() ? bucket.getKey() : bucket.getKeyAsText().string();
    }

    @Test
    public void testBasics() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(TERMS_AGGS_FIELD)
                        .subAggregation(
                                topHits("hits").addSort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                        )
                )
                .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        long higestSortValue = 0;
        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(10l));
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.totalHits(), equalTo(10l));
            assertThat(hits.getHits().length, equalTo(3));
            higestSortValue += 10;
            assertThat((Long) hits.getAt(0).sortValues()[0], equalTo(higestSortValue));
            assertThat((Long) hits.getAt(1).sortValues()[0], equalTo(higestSortValue - 1));
            assertThat((Long) hits.getAt(2).sortValues()[0], equalTo(higestSortValue - 2));
        }
    }

    @Test
    public void testSortByBucket() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                                .executionHint(randomExecutionHint())
                                .field(TERMS_AGGS_FIELD)
                                .order(Terms.Order.aggregation("hits", false))
                                .subAggregation(
                                        topHits("hits").addSort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                                )
                )
                .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        long higestSortValue = 0;
        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(10l));
            /*TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.totalHits(), equalTo(10l));
            assertThat(hits.getHits().length, equalTo(3));
            higestSortValue += 10;
            assertThat((Long) hits.getAt(0).sortValues()[0], equalTo(higestSortValue));
            assertThat((Long) hits.getAt(1).sortValues()[0], equalTo(higestSortValue - 1));
            assertThat((Long) hits.getAt(2).sortValues()[0], equalTo(higestSortValue - 2));*/
        }
    }

}
