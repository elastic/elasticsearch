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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.sampler.Sampler;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsNull.notNullValue;

@ElasticsearchIntegrationTest.SuiteScopeTest

public class SamplerTests extends ElasticsearchIntegrationTest {
    

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
        // Need to have consistent number of shards for scoring logic
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", between(0, 1))
                .build();
    }    

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        // Here we give a subset of docs a "bonus" field to introduce scoring
        // differences for later quality tests
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int num = (int) (Math.random() * 5);
            builders.add(client().prepareIndex("idx", "type").setSource(
                    jsonBuilder().startObject().field("num", num).field("tag", num % 2 == 0 ? "even" : "odd")
                            .field("bonus", num == 4 ? "highScore highScore highScore" : "")
                    .endObject()));
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");
        ensureSearchable();
    }
    
    

    @Test
    public void testBestQualitySample() throws Exception {
        int maxPerShardSize = 5;
        SearchResponse response = client().prepareSearch("idx").setQuery(QueryBuilders.matchQuery("num", 1))
                .addAggregation(
                        AggregationBuilders.sampler("sample").shardSize(maxPerShardSize)
                                .subAggregation(AggregationBuilders.terms("terms").field("tag").size(10))).execute().actionGet();
        assertSearchResponse(response);

        Sampler sample = response.getAggregations().get("sample");
        assertThat(sample.getDocCount(), lessThanOrEqualTo((long) getNumShards("idx").numPrimaries * maxPerShardSize));
        Terms terms = sample.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        Bucket odds = terms.getBucketByKey("odd");
        assertNotNull(odds);
        Bucket evens = terms.getBucketByKey("even");
        assertNull(evens);
    }

    @Test
    public void testRandomSample() throws Exception {
        // Runs an agg on a random sample of results.
        int maxPerShardSize = 5;
        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(QueryBuilders.matchQuery("_all", "even highScore"))
                .addAggregation(
                        AggregationBuilders.sampler("sample").shardSize(maxPerShardSize).useRandomSample(true)
                                .subAggregation(AggregationBuilders.terms("terms").field("num").size(10))).execute().actionGet();
        assertSearchResponse(response);

        Sampler sample = response.getAggregations().get("sample");
        assertThat(sample.getDocCount(), lessThanOrEqualTo((long) getNumShards("idx").numPrimaries * maxPerShardSize));
        Terms terms = sample.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));

        String[] nonExpecteds = { "one", "three", "five" };
        for (String nonExpectedNum : nonExpecteds) {
            assertNull(terms.getBucketByKey(nonExpectedNum));
        }
        // We expect diversity in our results which is missing if we purely rely
        // on Lucene-ranked selections
        assertTrue("A random sample should include lower-ranked results (this test may fail in VERY rare cases)",
                terms.getBuckets().size() > 1);
    }


    @Test
    public void testNestedSample() throws Exception {
        int maxPerShardSize = 500;
        SearchResponse response = client()
                .prepareSearch("idx")
                .setQuery(QueryBuilders.matchQuery("_all", "even odd"))
                .addAggregation(
                        AggregationBuilders
                                .terms("tags")
                                .field("tag")
                                .subAggregation(
                                        AggregationBuilders.sampler("sample").shardSize(maxPerShardSize)
                                                .subAggregation(AggregationBuilders.terms("nums").field("num").size(10)))).execute()
                .actionGet();
        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("tags");
        List<Bucket> tagsList = tags.getBuckets();
        assertThat(tagsList.size(), equalTo(2));
        checkSamples(maxPerShardSize, tags.getBucketByKey("odd").getAggregations(), 1);
        checkSamples(maxPerShardSize, tags.getBucketByKey("even").getAggregations(), 0);
    }

    private void checkSamples(int maxPerShardSize, Aggregations oddEvenBucket, int expectedModulo) {
        Sampler sample = oddEvenBucket.get("sample");
        assertThat(sample.getDocCount(), lessThanOrEqualTo((long) getNumShards("idx").numPrimaries * maxPerShardSize));
        Terms nums = sample.getAggregations().get("nums");
        assertThat(nums, notNullValue());
        List<Bucket> numBuckets = nums.getBuckets();
        for (Bucket bucket : numBuckets) {
            int key = Integer.parseInt(bucket.getKey());
            assertThat(key % 2, equalTo(expectedModulo));
        }
    }

}
