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
package org.elasticsearch.search.facet.terms;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.ShardSizeTests;
import org.elasticsearch.search.facet.Facets;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.facet.FacetBuilders.termsFacet;
import static org.hamcrest.Matchers.equalTo;

public class ShardSizeTermsFacetTests extends ShardSizeTests {

    @Test
    public void noShardSize_string() throws Exception {

        createIdx("type=string,index=not_analyzed");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<String, Integer> expected = ImmutableMap.<String, Integer>builder()
                .put("1", 8)
                .put("3", 8)
                .put("2", 4)
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTerm().string())));
        }
    }

    @Test
    public void withShardSize_string() throws Exception {

        createIdx("type=string,index=not_analyzed");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).shardSize(5).order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Integer> expected = ImmutableMap.<String, Integer>builder()
                .put("1", 8)
                .put("3", 8)
                .put("2", 5) // <-- count is now fixed
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTerm().string())));
        }
    }

    @Test
    public void withShardSize_string_singleShard() throws Exception {

        createIdx("type=string,index=not_analyzed");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type").setRouting("1")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).shardSize(5).order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Integer> expected = ImmutableMap.<String, Integer>builder()
                .put("1", 5)
                .put("2", 4)
                .put("3", 3) // <-- count is now fixed
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTerm().string())));
        }
    }

    @Test
    public void withShardSize_string_withExecutionHintMap() throws Exception {

        createIdx("type=string,index=not_analyzed");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).shardSize(5).executionHint("map").order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Integer> expected = ImmutableMap.<String, Integer>builder()
                .put("1", 8)
                .put("3", 8)
                .put("2", 5) // <-- count is now fixed
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTerm().string())));
        }
    }

    @Test
    public void withShardSize_string_withExecutionHintMap_singleShard() throws Exception {

        createIdx("type=string,index=not_analyzed");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type").setRouting("1")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).shardSize(5).executionHint("map").order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Integer> expected = ImmutableMap.<String, Integer>builder()
                .put("1", 5)
                .put("2", 4)
                .put("3", 3) // <-- count is now fixed
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTerm().string())));
        }
    }

    @Test
    public void noShardSize_long() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<Integer, Integer> expected = ImmutableMap.<Integer, Integer>builder()
                .put(1, 8)
                .put(3, 8)
                .put(2, 4)
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
        }
    }

    @Test
    public void withShardSize_long() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).shardSize(5).order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<Integer, Integer> expected = ImmutableMap.<Integer, Integer>builder()
                .put(1, 8)
                .put(3, 8)
                .put(2, 5) // <-- count is now fixed
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
        }
    }

    @Test
    public void withShardSize_long_singleShard() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type").setRouting("1")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).shardSize(5).order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<Integer, Integer> expected = ImmutableMap.<Integer, Integer>builder()
                .put(1, 5)
                .put(2, 4)
                .put(3, 3)
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
        }
    }

    @Test
    public void noShardSize_double() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3));
        Map<Integer, Integer> expected = ImmutableMap.<Integer, Integer>builder()
                .put(1, 8)
                .put(3, 8)
                .put(2, 4)
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
        }
    }

    @Test
    public void withShardSize_double() throws Exception {

        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).shardSize(5).order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<Integer, Integer> expected = ImmutableMap.<Integer, Integer>builder()
                .put(1, 8)
                .put(3, 8)
                .put(2, 5) // <-- count is now fixed
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
        }
    }

    @Test
    public void withShardSize_double_singleShard() throws Exception {

        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx").setTypes("type").setRouting("1")
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("keys").field("key").size(3).shardSize(5).order(TermsFacet.ComparatorType.COUNT))
                .execute().actionGet();

        Facets facets = response.getFacets();
        TermsFacet terms = facets.facet("keys");
        List<? extends TermsFacet.Entry> entries = terms.getEntries();
        assertThat(entries.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<Integer, Integer> expected = ImmutableMap.<Integer, Integer>builder()
                .put(1, 5)
                .put(2, 4)
                .put(3, 3)
                .build();
        for (TermsFacet.Entry entry : entries) {
            assertThat(entry.getCount(), equalTo(expected.get(entry.getTermAsNumber().intValue())));
        }
    }
}
