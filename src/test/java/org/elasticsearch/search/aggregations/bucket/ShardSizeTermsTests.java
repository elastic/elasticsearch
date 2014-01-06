/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope = Scope.TEST)
public class ShardSizeTermsTests extends ElasticsearchIntegrationTest {

    /**
     * to properly test the effect/functionality of shard_size, we need to force having 2 shards and also
     * control the routing such that certain documents will end on each shard. Using "djb" routing hash + ignoring the
     * doc type when hashing will ensure that docs with routing value "1" will end up in a different shard than docs with
     * routing value "2".
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
                .put("cluster.routing.operation.hash.type", "djb")
                .put("cluster.routing.operation.use_type", "false")
                .build();
    }

    @Test
    public void noShardSize_string() throws Exception {
        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=string,index=not_analyzed")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).order(Terms.Order.COUNT_DESC))
                .execute().actionGet();

        Terms  terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.buckets();
        assertThat(buckets.size(), equalTo(3));
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 8l)
                .put("3", 8l)
                .put("2", 4l)
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKey().string())));
        }
    }

    @Test
    public void withShardSize_string() throws Exception {

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=string,index=not_analyzed")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).shardSize(5).order(Terms.Order.COUNT_DESC))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.buckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 8l)
                .put("3", 8l)
                .put("2", 5l) // <-- count is now fixed
                .build();
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKey().string())));
        }
    }

    @Test
    public void withShardSize_string_singleShard() throws Exception {

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=string,index=not_analyzed")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type").setRouting("1")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).shardSize(5).order(Terms.Order.COUNT_DESC))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.buckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 5l)
                .put("2", 4l)
                .put("3", 3l) // <-- count is now fixed
                .build();
        for (Terms.Bucket bucket: buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKey().string())));
        }
    }

    @Test
    public void noShardSize_long() throws Exception {

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=long")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).order(Terms.Order.COUNT_DESC))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.buckets();
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

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=long")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).shardSize(5).order(Terms.Order.COUNT_DESC))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.buckets();
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

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=long")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type").setRouting("1")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).shardSize(5).order(Terms.Order.COUNT_DESC))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.buckets();
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
    public void noShardSize_double() throws Exception {

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=double")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).order(Terms.Order.COUNT_DESC))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.buckets();
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

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=double")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).shardSize(5).order(Terms.Order.COUNT_DESC))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.buckets();
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

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=double")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type").setRouting("1")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(3).shardSize(5).order(Terms.Order.COUNT_DESC))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("keys");
        Collection<Terms.Bucket> buckets = terms.buckets();
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

    private void indexData() throws Exception {

        /*


        ||          ||           size = 3, shard_size = 5               ||           shard_size = size = 3               ||
        ||==========||==================================================||===============================================||
        || shard 1: ||  "1" - 5 | "2" - 4 | "3" - 3 | "4" - 2 | "5" - 1 || "1" - 5 | "3" - 3 | "2" - 4                   ||
        ||----------||--------------------------------------------------||-----------------------------------------------||
        || shard 2: ||  "1" - 3 | "2" - 1 | "3" - 5 | "4" - 2 | "5" - 1 || "1" - 3 | "3" - 5 | "4" - 2                   ||
        ||----------||--------------------------------------------------||-----------------------------------------------||
        || reduced: ||  "1" - 8 | "2" - 5 | "3" - 8 | "4" - 4 | "5" - 2 ||                                               ||
        ||          ||                                                  || "1" - 8, "3" - 8, "2" - 4    <= WRONG         ||
        ||          ||  "1" - 8 | "3" - 8 | "2" - 5     <= CORRECT      ||                                               ||


        */

        List<IndexRequestBuilder> indexOps = new ArrayList<IndexRequestBuilder>();

        indexDoc("1", "1", 5, indexOps);
        indexDoc("1", "2", 4, indexOps);
        indexDoc("1", "3", 3, indexOps);
        indexDoc("1", "4", 2, indexOps);
        indexDoc("1", "5", 1, indexOps);

        // total docs in shard "1" = 15

        indexDoc("2", "1", 3, indexOps);
        indexDoc("2", "2", 1, indexOps);
        indexDoc("2", "3", 5, indexOps);
        indexDoc("2", "4", 2, indexOps);
        indexDoc("2", "5", 1, indexOps);

        // total docs in shard "2"  = 12

        indexRandom(true, indexOps);

        long totalOnOne = client().prepareSearch("idx").setTypes("type").setRouting("1").setQuery(matchAllQuery()).execute().actionGet().getHits().getTotalHits();
        assertThat(totalOnOne, is(15l));
        long totalOnTwo = client().prepareSearch("idx").setTypes("type").setRouting("2").setQuery(matchAllQuery()).execute().actionGet().getHits().getTotalHits();
        assertThat(totalOnTwo, is(12l));
        ensureSearchable();
    }

    private void indexDoc(String shard, String key, int times, List<IndexRequestBuilder> indexOps) throws Exception {
        for (int i = 0; i < times; i++) {
            indexOps.add(client().prepareIndex("idx", "type").setRouting(shard).setCreate(true).setSource(jsonBuilder()
                    .startObject()
                    .field("key", key)
                    .endObject()));
        }
    }

}
