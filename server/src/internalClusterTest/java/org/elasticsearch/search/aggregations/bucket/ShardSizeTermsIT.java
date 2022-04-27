/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.equalTo;

public class ShardSizeTermsIT extends ShardSizeTestCase {
    public void testNoShardSizeString() throws Exception {
        createIdx("type=keyword");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<String, Long> expected = new HashMap<>();
        expected.put("1", 8L);
        expected.put("3", 8L);
        expected.put("2", 5L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }

    public void testShardSizeEqualsSizeString() throws Exception {
        createIdx("type=keyword");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .shardSize(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<String, Long> expected = new HashMap<>();
        expected.put("1", 8L);
        expected.put("3", 8L);
        expected.put("2", 4L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }

    public void testWithShardSizeString() throws Exception {

        createIdx("type=keyword");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Long> expected = new HashMap<>();
        expected.put("1", 8L);
        expected.put("3", 8L);
        expected.put("2", 5L); // <-- count is now fixed
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }

    public void testWithShardSizeStringSingleShard() throws Exception {

        createIdx("type=keyword");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setRouting(routing1)
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Long> expected = new HashMap<>();
        expected.put("1", 5L);
        expected.put("2", 4L);
        expected.put("3", 3L); // <-- count is now fixed
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKey())));
        }
    }

    public void testNoShardSizeTermOrderString() throws Exception {
        createIdx("type=keyword");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.key(true))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<String, Long> expected = new HashMap<>();
        expected.put("1", 8L);
        expected.put("2", 5L);
        expected.put("3", 8L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }

    public void testNoShardSizeLong() throws Exception {
        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 5L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testShardSizeEqualsSizeLong() throws Exception {
        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .shardSize(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 4L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testWithShardSizeLong() throws Exception {
        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 5L); // <-- count is now fixed
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testWithShardSizeLongSingleShard() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setRouting(routing1)
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 5L);
        expected.put(2, 4L);
        expected.put(3, 3L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testNoShardSizeTermOrderLong() throws Exception {
        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.key(true))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(2, 5L);
        expected.put(3, 8L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testNoShardSizeDouble() throws Exception {
        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 5L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testShardSizeEqualsSizeDouble() throws Exception {
        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .shardSize(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 4L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testWithShardSizeDouble() throws Exception {
        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 5L); // <-- count is now fixed
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testWithShardSizeDoubleSingleShard() throws Exception {
        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setRouting(routing1)
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 5L);
        expected.put(2, 4L);
        expected.put(3, 3L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testNoShardSizeTermOrderDouble() throws Exception {
        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.key(true))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(2, 5L);
        expected.put(3, 8L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }
}
