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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class NestedTests extends ElasticsearchIntegrationTest {

    static int numParents;
    static int[] numChildren;
    static SubAggCollectionMode aggCollectionMode;

    @Override
    public void setupSuiteScopeCluster() throws Exception {

        assertAcked(prepareCreate("idx")
                .addMapping("type", "nested", "type=nested"));

        List<IndexRequestBuilder> builders = new ArrayList<>();

        numParents = randomIntBetween(3, 10);
        numChildren = new int[numParents];
        aggCollectionMode = randomFrom(SubAggCollectionMode.values());
        logger.info("AGG COLLECTION MODE: " + aggCollectionMode);
        int totalChildren = 0;
        for (int i = 0; i < numParents; ++i) {
            if (i == numParents - 1 && totalChildren == 0) {
                // we need at least one child overall
                numChildren[i] = randomIntBetween(1, 5);
            } else {
                numChildren[i] = randomInt(5);
            }
            totalChildren += numChildren[i];
        }
        assertTrue(totalChildren > 0);

        for (int i = 0; i < numParents; i++) {
            XContentBuilder source = jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .startArray("nested");
            for (int j = 0; j < numChildren[i]; ++j) {
                source = source.startObject().field("value", i + 1 + j).endObject();
            }
            source = source.endArray().endObject();
            builders.add(client().prepareIndex("idx", "type", ""+i+1).setSource(source));
        }

        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer", "nested", "type=nested").execute().actionGet();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i*2)
                    .startArray("nested")
                    .startObject().field("value", i + 1).endObject()
                    .startObject().field("value", i + 2).endObject()
                    .startObject().field("value", i + 3).endObject()
                    .startObject().field("value", i + 4).endObject()
                    .startObject().field("value", i + 5).endObject()
                    .endArray()
                    .endObject()));
        }

        assertAcked(prepareCreate("idx_nested_nested_aggs")
                .addMapping("type", jsonBuilder().startObject().startObject("type").startObject("properties")
                        .startObject("nested1")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("nested2")
                                    .field("type", "nested")
                                .endObject()
                            .endObject()
                        .endObject()
                        .endObject().endObject().endObject()));

        builders.add(
                client().prepareIndex("idx_nested_nested_aggs", "type", "1")
                        .setSource(jsonBuilder().startObject()
                                .startArray("nested1")
                                    .startObject()
                                    .field("a", "a")
                                        .startArray("nested2")
                                            .startObject()
                                                .field("b", 2)
                                            .endObject()
                                        .endArray()
                                    .endObject()
                                    .startObject()
                                        .field("a", "b")
                                        .startArray("nested2")
                                            .startObject()
                                                .field("b", 2)
                                            .endObject()
                                        .endArray()
                                    .endObject()
                                .endArray()
                            .endObject())
        );
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Test
    public void simple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(nested("nested").path("nested")
                        .subAggregation(stats("nested_value_stats").field("nested.value")))
                .execute().actionGet();

        assertSearchResponse(response);


        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long sum = 0;
        long count = 0;
        for (int i = 0; i < numParents; ++i) {
            for (int j = 0; j < numChildren[i]; ++j) {
                final long value = i + 1 + j;
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                ++count;
            }
        }

        Nested nested = response.getAggregations().get("nested");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), equalTo(count));
        assertThat(nested.getAggregations().asList().isEmpty(), is(false));

        Stats stats = nested.getAggregations().get("nested_value_stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getMin(), equalTo(min));
        assertThat(stats.getMax(), equalTo(max));
        assertThat(stats.getCount(), equalTo(count));
        assertThat(stats.getSum(), equalTo((double) sum));
        assertThat(stats.getAvg(), equalTo((double) sum / count));
    }

    @Test
    public void onNonNestedField() throws Exception {
        try {
            client().prepareSearch("idx")
                    .addAggregation(nested("nested").path("value")
                            .subAggregation(stats("nested_value_stats").field("nested.value")))
                    .execute().actionGet();

            fail("expected execution to fail - an attempt to nested facet on non-nested field/path");

        } catch (ElasticsearchException ese) {
        }
    }

    @Test
    public void nestedWithSubTermsAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(nested("nested").path("nested")
                        .subAggregation(terms("values").field("nested.value").size(100)
                                .collectMode(aggCollectionMode)))
                .execute().actionGet();

        assertSearchResponse(response);


        long docCount = 0;
        long[] counts = new long[numParents + 6];
        for (int i = 0; i < numParents; ++i) {
            for (int j = 0; j < numChildren[i]; ++j) {
                final int value = i + 1 + j;
                ++counts[value];
                ++docCount;
            }
        }
        int uniqueValues = 0;
        for (long count : counts) {
            if (count > 0) {
                ++uniqueValues;
            }
        }

        Nested nested = response.getAggregations().get("nested");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), equalTo(docCount));
        assertThat(nested.getAggregations().asList().isEmpty(), is(false));

        LongTerms values = nested.getAggregations().get("values");
        assertThat(values, notNullValue());
        assertThat(values.getName(), equalTo("values"));
        assertThat(values.getBuckets(), notNullValue());
        assertThat(values.getBuckets().size(), equalTo(uniqueValues));
        for (int i = 0; i < counts.length; ++i) {
            final String key = Long.toString(i);
            if (counts[i] == 0) {
                assertNull(values.getBucketByKey(key));
            } else {
                Bucket bucket = values.getBucketByKey(key);
                assertNotNull(bucket);
                assertEquals(counts[i], bucket.getDocCount());
            }
        }
    }

    @Test
    public void nestedAsSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("top_values").field("value").size(100)
                        .collectMode(aggCollectionMode)
                        .subAggregation(nested("nested").path("nested")
                                .subAggregation(max("max_value").field("nested.value"))))
                .execute().actionGet();

        assertSearchResponse(response);


        LongTerms values = response.getAggregations().get("top_values");
        assertThat(values, notNullValue());
        assertThat(values.getName(), equalTo("top_values"));
        assertThat(values.getBuckets(), notNullValue());
        assertThat(values.getBuckets().size(), equalTo(numParents));

        for (int i = 0; i < numParents; i++) {
            String topValue = "" + (i + 1);
            assertThat(values.getBucketByKey(topValue), notNullValue());
            Nested nested = values.getBucketByKey(topValue).getAggregations().get("nested");
            assertThat(nested, notNullValue());
            Max max = nested.getAggregations().get("max_value");
            assertThat(max, notNullValue());
            assertThat(max.getValue(), equalTo(numChildren[i] == 0 ? Double.NEGATIVE_INFINITY : (double) i + numChildren[i]));
        }
    }

    @Test
    public void nestNestedAggs() throws Exception {
        SearchResponse response = client().prepareSearch("idx_nested_nested_aggs")
                .addAggregation(nested("level1").path("nested1")
                        .subAggregation(terms("a").field("nested1.a")
                                .collectMode(aggCollectionMode)
                                .subAggregation(nested("level2").path("nested1.nested2")
                                        .subAggregation(sum("sum").field("nested1.nested2.b")))))
                .get();
        assertSearchResponse(response);


        Nested level1 = response.getAggregations().get("level1");
        assertThat(level1, notNullValue());
        assertThat(level1.getName(), equalTo("level1"));
        assertThat(level1.getDocCount(), equalTo(2l));

        StringTerms a = level1.getAggregations().get("a");
        Terms.Bucket bBucket = a.getBucketByKey("a");
        assertThat(bBucket.getDocCount(), equalTo(1l));

        Nested level2 = bBucket.getAggregations().get("level2");
        assertThat(level2.getDocCount(), equalTo(1l));
        Sum sum = level2.getAggregations().get("sum");
        assertThat(sum.getValue(), equalTo(2d));

        a = level1.getAggregations().get("a");
        bBucket = a.getBucketByKey("b");
        assertThat(bBucket.getDocCount(), equalTo(1l));

        level2 = bBucket.getAggregations().get("level2");
        assertThat(level2.getDocCount(), equalTo(1l));
        sum = level2.getAggregations().get("sum");
        assertThat(sum.getValue(), equalTo(2d));
    }


    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0)
                        .subAggregation(nested("nested").path("nested")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBucketByKey(1l);
        assertThat(bucket, Matchers.notNullValue());

        Nested nested = bucket.getAggregations().get("nested");
        assertThat(nested, Matchers.notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), is(0l));
    }
}
