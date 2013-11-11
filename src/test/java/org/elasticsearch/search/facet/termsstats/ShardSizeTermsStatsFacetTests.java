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

package org.elasticsearch.search.facet.termsstats;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.facet.FacetBuilders.termsStatsFacet;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE)
public class ShardSizeTermsStatsFacetTests extends ElasticsearchIntegrationTest {

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
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(3).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 8l)
                .put("3", 8l)
                .put("2", 4l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTerm().string())));
        }
    }

    @Test
    public void noShardSize_string_allTerms() throws Exception {

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=string,index=not_analyzed")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(0).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(5));
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 8l)
                .put("3", 8l)
                .put("2", 5l)
                .put("4", 4l)
                .put("5", 2l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTerm().string())));
        }
    }

    @Test
    public void withShardSize_string_allTerms() throws Exception {

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=string,index=not_analyzed")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(0).shardSize(3).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(5));
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 8l)
                .put("3", 8l)
                .put("2", 5l)
                .put("4", 4l)
                .put("5", 2l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTerm().string())));
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
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(3).shardSize(5).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 8l)
                .put("3", 8l)
                .put("2", 5l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTerm().string())));
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
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(3).shardSize(5).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<String, Long> expected = ImmutableMap.<String, Long>builder()
                .put("1", 5l)
                .put("2", 4l)
                .put("3", 3l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTerm().string())));
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
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(3).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 4l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
        }
    }

    @Test
    public void noShardSize_long_allTerms() throws Exception {

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=long")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(0).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(5));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 5l)
                .put(4, 4l)
                .put(5, 2l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
        }
    }

    @Test
    public void withShardSize_long_allTerms() throws Exception {

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=long")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(0).shardSize(3).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(5));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 5l)
                .put(4, 4l)
                .put(5, 2l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
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
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(3).shardSize(5).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 5l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
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
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(3).shardSize(5).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 5l)
                .put(2, 4l)
                .put(3, 3l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
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
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(3).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 4l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
        }
    }

    @Test
    public void noShardSize_double_allTerms() throws Exception {

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=double")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(0).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(5));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 5l)
                .put(4, 4l)
                .put(5, 2l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
        }
    }

    @Test
    public void withShardSize_double_allTerms() throws Exception {

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "key", "type=double")
                .execute().actionGet();

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(0).shardSize(3).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(5));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 5l)
                .put(4, 4l)
                .put(5, 2l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
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
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(3).shardSize(5).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 8l)
                .put(3, 8l)
                .put(2, 5l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
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
                .addFacet(termsStatsFacet("keys").keyField("key").valueField("value").size(3).shardSize(5).order(TermsStatsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsStatsFacet facet = facets.facet("keys");
        List<? extends TermsStatsFacet.Entry> entries = facet.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder()
                .put(1, 5l)
                .put(2, 4l)
                .put(3, 3l)
                .build();
        for (TermsStatsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
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


        indexDoc("1", "1", 5);
        indexDoc("1", "2", 4);
        indexDoc("1", "3", 3);
        indexDoc("1", "4", 2);
        indexDoc("1", "5", 1);

        // total docs in shard "1" = 15

        indexDoc("2", "1", 3);
        indexDoc("2", "2", 1);
        indexDoc("2", "3", 5);
        indexDoc("2", "4", 2);
        indexDoc("2", "5", 1);

        // total docs in shard "2"  = 12

        client().admin().indices().prepareFlush("idx").execute().actionGet();
        client().admin().indices().prepareRefresh("idx").execute().actionGet();

        long totalOnOne = client().prepareSearch("idx").setTypes("type").setRouting("1").setQuery(matchAllQuery()).execute().actionGet().getHits().getTotalHits();
        assertThat(totalOnOne, is(15l));
        long totalOnTwo = client().prepareSearch("idx").setTypes("type").setRouting("2").setQuery(matchAllQuery()).execute().actionGet().getHits().getTotalHits();
        assertThat(totalOnTwo, is(12l));
    }

    private void indexDoc(String shard, String key, int times) throws Exception {
        for (int i = 0; i < times; i++) {
            client().prepareIndex("idx", "type").setRouting(shard).setCreate(true).setSource(jsonBuilder()
                    .startObject()
                    .field("key", key)
                    .field("value", 1)
                    .endObject()).execute().actionGet();
        }
    }

}
