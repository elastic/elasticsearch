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

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.equalTo;

public class ShardSizeTermsIT extends ShardSizeTestCase {

    @Test
    public void noShardSize_string() throws Exception {
        createIdx("type=string,index=not_analyzed");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms  terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 8l)
                .put("3", 8l)
                .put("2", 5l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }
    
    @Test
    public void shardSizeEqualsSize_string() throws Exception {
        createIdx("type=string,index=not_analyzed");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).shardSize(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms  terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 8l)
                .put("3", 8l)
                .put("2", 4l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }

    @Test
    public void withShardSize_string() throws Exception {

        createIdx("type=string,index=not_analyzed");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).shardSize(5).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 8l)
                .put("3", 8l)
                .put("2", 5l) // <-- count is now fixed
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }

    @Test
    public void withShardSize_string_singleShard() throws Exception {

        createIdx("type=string,index=not_analyzed");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type").setRouting(routing1)
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).shardSize(5).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 5l)
                .put("2", 4l)
                .put("3", 3l) // <-- count is now fixed
                .build();
        for (Terms.Bucket bucket: buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKey())));
        }
    }
    
    @Test
    public void noShardSizeTermOrder_string() throws Exception {
        createIdx("type=string,index=not_analyzed");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.term(true)))
                .execute().actionGet();

        Terms  terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 8l)
                .put("2", 5l)
                .put("3", 8l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }

    @Test
    public void noShardSize_long() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 5l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    @Test
    public void shardSizeEqualsSize_long() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).shardSize(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 4l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    @Test
    public void withShardSize_long() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).shardSize(5).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 5l) // <-- count is now fixed
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    @Test
    public void withShardSize_long_singleShard() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type").setRouting(routing1)
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).shardSize(5).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 5l)
                .put(2, 4l)
                .put(3, 3l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    @Test
    public void noShardSizeTermOrder_long() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.term(true)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(2, 5l)
                .put(3, 8l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    @Test
    public void noShardSize_double() throws Exception {

        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 5l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    @Test
    public void shardSizeEqualsSize_double() throws Exception {

        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).shardSize(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 4l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    @Test
    public void withShardSize_double() throws Exception {

        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).shardSize(5).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 5l) // <-- count is now fixed
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    @Test
    public void withShardSize_double_singleShard() throws Exception {

        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type").setRouting(routing1)
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).shardSize(5).order(Terms.Order.count(false)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 5l)
                .put(2, 4l)
                .put(3, 3l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    @Test
    public void noShardSizeTermOrder_double() throws Exception {

        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.term(true)))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(2, 5l)
                .put(3, 8l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }
}
