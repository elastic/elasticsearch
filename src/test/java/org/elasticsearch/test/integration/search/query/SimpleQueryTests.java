/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.search.query;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 *
 */
public class SimpleQueryTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void passQueryAsStringTest() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery("{ \"term\" : { \"field1\" : \"value1_1\" }}").execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
    }

    @Test
    public void testIndexOptions() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1").field("index_options", "docs").field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox").execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox").setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(QueryBuilders.matchQuery("field2", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        try {
            client.prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).execute().actionGet();
        } catch (SearchPhaseExecutionException e) {
            assertTrue(e.getMessage().endsWith("IllegalStateException[field \"field1\" was indexed without position data; cannot run PhraseQuery (term=quick)]; }"));
        }
    }

    @Test
    public void testOmitTermFreqsAndPositions() throws Exception {
        // backwards compat test!
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1").field("omit_term_freq_and_positions", true).field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox").execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox").setRefresh(true).execute().actionGet();


        SearchResponse searchResponse = client.prepareSearch().setQuery(QueryBuilders.matchQuery("field2", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        try {
            client.prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).execute().actionGet();
        } catch (SearchPhaseExecutionException e) {
            assertTrue(e.getMessage().endsWith("IllegalStateException[field \"field1\" was indexed without position data; cannot run PhraseQuery (term=quick)]; }"));
        }
    }

    @Test
    public void queryStringAnalyzedWildcard() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(queryString("value*").analyzeWildcard(true)).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch().setQuery(queryString("*ue*").analyzeWildcard(true)).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch().setQuery(queryString("*ue_1").analyzeWildcard(true)).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch().setQuery(queryString("val*e_1").analyzeWildcard(true)).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch().setQuery(queryString("v?l*e?1").analyzeWildcard(true)).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
    }

    @Test
    public void typeFilterTypeIndexedTests() throws Exception {
        typeFilterTests("not_analyzed");
    }

    @Test
    public void typeFilterTypeNotIndexedTests() throws Exception {
        typeFilterTests("no");
    }

    private void typeFilterTests(String index) throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                        .startObject("_type").field("index", index).endObject()
                        .endObject().endObject())
                .addMapping("type2", jsonBuilder().startObject().startObject("type2")
                        .startObject("_type").field("index", index).endObject()
                        .endObject().endObject())
                .execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
        client.prepareIndex("test", "type2", "1").setSource("field1", "value1").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();

        client.prepareIndex("test", "type1", "2").setSource("field1", "value1").execute().actionGet();
        client.prepareIndex("test", "type2", "2").setSource("field1", "value1").execute().actionGet();
        client.prepareIndex("test", "type2", "3").setSource("field1", "value1").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        assertThat(client.prepareCount().setQuery(filteredQuery(matchAllQuery(), typeFilter("type1"))).execute().actionGet().count(), equalTo(2l));
        assertThat(client.prepareCount().setQuery(filteredQuery(matchAllQuery(), typeFilter("type2"))).execute().actionGet().count(), equalTo(3l));

        assertThat(client.prepareCount().setTypes("type1").setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(2l));
        assertThat(client.prepareCount().setTypes("type2").setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(3l));

        assertThat(client.prepareCount().setTypes("type1", "type2").setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(5l));
    }

    @Test
    public void idsFilterTestsIdIndexed() throws Exception {
        idsFilterTests("not_analyzed");
    }

    @Test
    public void idsFilterTestsIdNotIndexed() throws Exception {
        idsFilterTests("no");
    }

    private void idsFilterTests(String index) throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                        .startObject("_id").field("index", index).endObject()
                        .endObject().endObject())
                .execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource("field1", "value2").execute().actionGet();
        client.prepareIndex("test", "type1", "3").setSource("field1", "value3").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(constantScoreQuery(idsFilter("type1").ids("1", "3"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        // no type
        searchResponse = client.prepareSearch().setQuery(constantScoreQuery(idsFilter().ids("1", "3"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        searchResponse = client.prepareSearch().setQuery(idsQuery("type1").ids("1", "3")).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        // no type
        searchResponse = client.prepareSearch().setQuery(idsQuery().ids("1", "3")).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        searchResponse = client.prepareSearch().setQuery(idsQuery("type1").ids("7", "10")).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(0l));

        // repeat..., with terms
        searchResponse = client.prepareSearch().setTypes("type1").setQuery(constantScoreQuery(termsFilter("_id", "1", "3"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));
    }

    @Test
    public void testLimitFilter() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "value1_1").execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource("field1", "value1_2").execute().actionGet();
        client.prepareIndex("test", "type1", "3").setSource("field2", "value2_3").execute().actionGet();
        client.prepareIndex("test", "type1", "4").setSource("field3", "value3_4").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), limitFilter(2))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
    }

    @Test
    public void filterExistsMissingTests() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource("field1", "value1_2").execute().actionGet();
        client.prepareIndex("test", "type1", "3").setSource("field2", "value2_3").execute().actionGet();
        client.prepareIndex("test", "type1", "4").setSource("field3", "value3_4").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client.prepareSearch().setQuery(constantScoreQuery(existsFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client.prepareSearch().setQuery(queryString("_exists_:field1")).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field2"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field3"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("4"));

        searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        // double check for cache
        searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        searchResponse = client.prepareSearch().setQuery(constantScoreQuery(missingFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        searchResponse = client.prepareSearch().setQuery(queryString("_missing_:field1")).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));
    }

    @Test
    public void passQueryOrFilterAsJSONStringTest() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefresh(true).execute().actionGet();

        WrapperQueryBuilder wrapper = new WrapperQueryBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }");
        SearchResponse searchResponse = client.prepareSearch().setQuery(wrapper).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        BoolQueryBuilder bool = new BoolQueryBuilder();
        bool.must(wrapper);
        bool.must(new TermQueryBuilder("field2", "value2_1"));

        searchResponse = client.prepareSearch().setQuery(wrapper).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        WrapperFilterBuilder wrapperFilter = new WrapperFilterBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }");

        searchResponse = client.prepareSearch().setFilter(wrapperFilter).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
    }

    @Test
    public void testFiltersWithCustomCacheKey() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1"))).execute().actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1"))).execute().actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1"))).execute().actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1"))).execute().actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
    }

    @Test
    public void testMultiMatchQuery() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value4", "field3", "value3").execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource("field1", "value2", "field2", "value5", "field3", "value2").execute().actionGet();
        client.prepareIndex("test", "type1", "3").setSource("field1", "value3", "field2", "value6", "field3", "value1").execute().actionGet();
        client.admin().indices().prepareRefresh("test").execute().actionGet();

        MultiMatchQueryBuilder builder = QueryBuilders.multiMatchQuery("value1 value2 value4", "field1", "field2");
        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(builder)
                .addFacet(FacetBuilders.termsFacet("field1").field("field1"))
                .execute().actionGet();

        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat("1", equalTo(searchResponse.hits().getAt(0).id()));
        assertThat("2", equalTo(searchResponse.hits().getAt(1).id()));

        builder.useDisMax(false);
        searchResponse = client.prepareSearch()
                .setQuery(builder)
                .execute().actionGet();

        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat("1", equalTo(searchResponse.hits().getAt(0).id()));
        assertThat("2", equalTo(searchResponse.hits().getAt(1).id()));

        client.admin().indices().prepareRefresh("test").execute().actionGet();
        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field2")
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client.prepareSearch()
                .setQuery(builder)
                .execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat("1", equalTo(searchResponse.hits().getAt(0).id()));

        client.admin().indices().prepareRefresh("test").execute().actionGet();
        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field3^1.5")
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client.prepareSearch()
                .setQuery(builder)
                .execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat("3", equalTo(searchResponse.hits().getAt(0).id()));
        assertThat("1", equalTo(searchResponse.hits().getAt(1).id()));

        client.admin().indices().prepareRefresh("test").execute().actionGet();
        builder = QueryBuilders.multiMatchQuery("value1").field("field1").field("field3", 1.5f)
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client.prepareSearch()
                .setQuery(builder)
                .execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat("3", equalTo(searchResponse.hits().getAt(0).id()));
        assertThat("1", equalTo(searchResponse.hits().getAt(1).id()));

        // Test lenient
        client.prepareIndex("test", "type1", "3").setSource("field1", "value7", "field2", "value8", "field4", 5).execute().actionGet();
        client.admin().indices().prepareRefresh("test").execute().actionGet();

        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field2", "field4");
        try {
            client.prepareSearch()
                    .setQuery(builder)
                    .execute().actionGet();
            fail("Exception expected");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.shardFailures()[0].status(), equalTo(RestStatus.BAD_REQUEST));
        }

        builder.lenient(true);
        searchResponse = client.prepareSearch().setQuery(builder).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat("1", equalTo(searchResponse.hits().getAt(0).id()));
    }

    @Test
    public void testMatchQueryZeroTermsQuery() {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();
        client.prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource("field1", "value2").execute().actionGet();
        client.admin().indices().prepareRefresh("test").execute().actionGet();

        BoolQueryBuilder boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE));
        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(0l));

        boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        searchResponse = client.prepareSearch()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        searchResponse = client.prepareSearch()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
    }

}
