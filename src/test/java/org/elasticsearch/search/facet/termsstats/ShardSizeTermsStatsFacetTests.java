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
package org.elasticsearch.search.facet.termsstats;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.ShardSizeTests;
import org.elasticsearch.search.facet.Facets;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.facet.FacetBuilders.termsStatsFacet;
import static org.hamcrest.Matchers.equalTo;

public class ShardSizeTermsStatsFacetTests extends ShardSizeTests {

    @Test
    public void noShardSize_string() throws Exception {

        createIdx("type=string,index=not_analyzed");

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

        createIdx("type=string,index=not_analyzed");

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

        createIdx("type=string,index=not_analyzed");

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

        createIdx("type=string,index=not_analyzed");

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

        createIdx("type=string,index=not_analyzed");

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

        createIdx("type=long");

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

        createIdx("type=long");

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

        createIdx("type=long");

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

        createIdx("type=long");

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

        createIdx("type=long");

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

        createIdx("type=double");

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

        createIdx("type=double");

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

        createIdx("type=double");

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

        createIdx("type=double");

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

        createIdx("type=double");

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
}
