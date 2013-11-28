/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class NestedTests extends ElasticsearchIntegrationTest {

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
                .put("index.number_of_shards", between(1, 5))
                .put("index.number_of_replicas", between(0, 1))
                .build();
    }

    int numParents;
    int[] numChildren;

    @Before
    public void init() throws Exception {

        prepareCreate("idx")
                .addMapping("type", "nested", "type=nested")
                .setSettings(indexSettings())
                .execute().actionGet();
        List<IndexRequestBuilder> builders = new ArrayList<IndexRequestBuilder>();

        numParents = randomIntBetween(3, 10);
        numChildren = new int[numParents];
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
        assert totalChildren > 0;

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
        indexRandom(true, builders);
    }

    @Test
    public void simple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(nested("nested").path("nested")
                        .subAggregation(stats("nested_value_stats").field("nested.value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

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

        } catch (ElasticSearchException ese) {
        }
    }

    @Test
    public void nestedWithSubTermsAgg() throws Exception {

        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(nested("nested").path("nested")
                        .subAggregation(terms("values").field("nested.value").size(100)))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

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
        assertThat(values.buckets(), notNullValue());
        assertThat(values.buckets().size(), equalTo(uniqueValues));
        for (int i = 0; i < counts.length; ++i) {
            final String key = Long.toString(i);
            if (counts[i] == 0) {
                assertNull(values.getByTerm(key));
            } else {
                Bucket bucket = values.getByTerm(key);
                assertNotNull(bucket);
                assertEquals(counts[i], bucket.getDocCount());
            }
        }
    }

    @Test
    public void nestedAsSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("top_values").field("value").size(100)
                        .subAggregation(nested("nested").path("nested")
                                .subAggregation(max("max_value").field("nested.value"))))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        LongTerms values = response.getAggregations().get("top_values");
        assertThat(values, notNullValue());
        assertThat(values.getName(), equalTo("top_values"));
        assertThat(values.buckets(), notNullValue());
        assertThat(values.buckets().size(), equalTo(numParents));

        for (int i = 0; i < numParents; i++) {
            String topValue = "" + (i + 1);
            assertThat(values.getByTerm(topValue), notNullValue());
            Nested nested = values.getByTerm(topValue).getAggregations().get("nested");
            assertThat(nested, notNullValue());
            Max max = nested.getAggregations().get("max_value");
            assertThat(max, notNullValue());
            assertThat(max.getValue(), equalTo(numChildren[i] == 0 ? Double.NEGATIVE_INFINITY : (double) i + numChildren[i]));
        }
    }

    @Test
    public void emptyAggregation() throws Exception {
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer", "nested", "type=nested").execute().actionGet();
        List<IndexRequestBuilder> builders = new ArrayList<IndexRequestBuilder>();
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
        indexRandom(true, builders.toArray(new IndexRequestBuilder[builders.size()]));

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).emptyBuckets(true)
                        .subAggregation(nested("nested").path("nested")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getByKey(1l);
        assertThat(bucket, Matchers.notNullValue());

        Nested nested = bucket.getAggregations().get("nested");
        assertThat(nested, Matchers.notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), is(0l));
    }
}
