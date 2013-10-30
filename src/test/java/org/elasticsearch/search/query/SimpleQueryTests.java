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

package org.elasticsearch.search.query;

import org.apache.lucene.util.English;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.CommonTermsQueryBuilder.Operator;
import org.elasticsearch.index.query.MatchQueryBuilder.Type;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleQueryTests extends AbstractIntegrationTest {

    @Test // see https://github.com/elasticsearch/elasticsearch/issues/3177
    public void testIssue3177() {
        run(prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)));
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2").execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource("field1", "value3").execute().actionGet();
        ensureGreen();
        waitForRelocation();
        optimize();
        refresh();
        assertThat(
                client().prepareSearch()
                        .setQuery(matchAllQuery())
                        .setFilter(
                                FilterBuilders.andFilter(
                                        FilterBuilders.queryFilter(matchAllQuery()),
                                        FilterBuilders.notFilter(FilterBuilders.andFilter(FilterBuilders.queryFilter(QueryBuilders.termQuery("field1", "value1")),
                                                FilterBuilders.queryFilter(QueryBuilders.termQuery("field1", "value2")))))).execute().actionGet().getHits().getTotalHits(),
                equalTo(3l));
        assertThat(
                client().prepareSearch()
                        .setQuery(
                                QueryBuilders.filteredQuery(
                                        QueryBuilders.boolQuery().should(QueryBuilders.termQuery("field1", "value1")).should(QueryBuilders.termQuery("field1", "value2"))
                                                .should(QueryBuilders.termQuery("field1", "value3")),
                                        FilterBuilders.notFilter(FilterBuilders.andFilter(FilterBuilders.queryFilter(QueryBuilders.termQuery("field1", "value1")),
                                                FilterBuilders.queryFilter(QueryBuilders.termQuery("field1", "value2")))))).execute().actionGet().getHits().getTotalHits(),
                equalTo(3l));
        assertThat(client().prepareSearch().setQuery(matchAllQuery()).setFilter(FilterBuilders.notFilter(FilterBuilders.termFilter("field1", "value3"))).execute().actionGet()
                .getHits().getTotalHits(), equalTo(2l));
    }

    @Test
    public void passQueryAsStringTest() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings("index.number_of_shards", 1).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery("{ \"term\" : { \"field1\" : \"value1_1\" }}").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testIndexOptions() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,index_options=docs")
                .setSettings("index.number_of_shards", 1).get();

        client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox").setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchQuery("field2", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        try {
            client().prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).get();
        } catch (SearchPhaseExecutionException e) {
            assertTrue("wrong exception message " + e.getMessage(), e.getMessage().endsWith("IllegalStateException[field \"field1\" was indexed without position data; cannot run PhraseQuery (term=quick)]; }"));
        }
    }
    
    @Test // see #3521
    public void testConstantScoreQuery() throws Exception {
        Random random = getRandom();
        createIndex("test");
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox"), client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox"));
        ensureYellow();
        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("field1", "quick"))).get();
        SearchHits hits = searchResponse.getHits();
        assertThat(hits.totalHits(), equalTo(2l));
        for (SearchHit searchHit : hits) {
            assertThat(searchHit.getScore(), equalTo(1.0f));
        }
        
        searchResponse = client().prepareSearch("test").setQuery(
                QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).must(
                QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("field1", "quick")).boost(1.0f + getRandom().nextFloat()))).get();
        hits = searchResponse.getHits();
        assertThat(hits.totalHits(), equalTo(2l));
        assertThat(hits.getAt(0).score(), equalTo(hits.getAt(1).score()));
        
        client().prepareSearch("test").setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("field1", "quick")).boost(1.0f + getRandom().nextFloat())).get();
        hits = searchResponse.getHits();
        assertThat(hits.totalHits(), equalTo(2l));
        assertThat(hits.getAt(0).score(), equalTo(hits.getAt(1).score()));
        
        searchResponse = client().prepareSearch("test").setQuery(
                QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).must(
                QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("field1", "quick")).boost(1.0f + (random.nextBoolean()? 0.0f : random.nextFloat()))))).get();
        hits = searchResponse.getHits();
        assertThat(hits.totalHits(), equalTo(2l));
        assertThat(hits.getAt(0).score(), equalTo(hits.getAt(1).score()));
        for (SearchHit searchHit : hits) {
            assertThat(searchHit.getScore(), equalTo(1.0f));
        }
        
        int num = atLeast(100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[num];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type", "" + i).setSource("f", English.intToEnglish(i));
        }
        createIndex("test_1");
        indexRandom(true, builders);
        ensureYellow();
        int queryRounds = atLeast(10);
        for (int i = 0; i < queryRounds; i++) {
            MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("f", English.intToEnglish(between(0, num)));
            searchResponse = client().prepareSearch("test_1").setQuery(matchQuery).setSize(num).get();
            long totalHits = searchResponse.getHits().totalHits();
            hits = searchResponse.getHits();
            for (SearchHit searchHit : hits) {
                assertThat(searchHit.getScore(), equalTo(1.0f));
            }
            if (random.nextBoolean()) {
                searchResponse = client().prepareSearch("test_1").setQuery(
                        QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).must(
                        QueryBuilders.constantScoreQuery(matchQuery).boost(1.0f + (random.nextBoolean()? 0.0f : random.nextFloat())))).setSize(num).get();
                hits = searchResponse.getHits();    
            } else {
                FilterBuilder filter = FilterBuilders.queryFilter(matchQuery);
                searchResponse = client().prepareSearch("test_1").setQuery(
                        QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).must(
                        QueryBuilders.constantScoreQuery(filter).boost(1.0f + (random.nextBoolean()? 0.0f : random.nextFloat())))).setSize(num).get();
                hits = searchResponse.getHits();
            }
            assertThat(hits.totalHits(), equalTo(totalHits));
            if (totalHits > 1) {
                float expected = hits.getAt(0).score();
                for (SearchHit searchHit : hits) {
                    assertThat(searchHit.getScore(), equalTo(expected));
                }
            }
        }
    }
    
    @Test // see #3521 
    public void testAllDocsQueryString() throws InterruptedException, ExecutionException {
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_replicas", 0)).execute().actionGet();
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("foo", "bar"),
            client().prepareIndex("test", "type1", "2").setSource("foo", "bar")
        );
        int iters = atLeast(100);
        for (int i = 0; i < iters; i++) {
            SearchResponse searchResponse = client().prepareSearch("test")
                    .setQuery(queryString("*:*^10.0").boost(10.0f))
                    .execute().actionGet();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
            
            searchResponse = client().prepareSearch("test")
                    .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).must(
                            QueryBuilders.constantScoreQuery(QueryBuilders.matchAllQuery())))
                    .execute().actionGet();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
            assertThat((double)searchResponse.getHits().getAt(0).score(), closeTo(Math.sqrt(2), 0.1));
            assertThat((double)searchResponse.getHits().getAt(1).score(),closeTo(Math.sqrt(2), 0.1));
        }
    }


    @Test
    public void testCommonTermsQuery() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,analyzer=whitespace")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        indexRandom(true, client().prepareIndex("test", "type1", "3").setSource("field1", "quick lazy huge brown pidgin", "field2", "the quick lazy huge brown fox jumps over the tree"),
                client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "the quick lazy huge brown fox jumps over the tree")
        );

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.commonTerms("field1", "the quick brown").cutoffFrequency(3).lowFreqOperator(Operator.OR)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.commonTerms("field1", "the quick brown").cutoffFrequency(3).lowFreqOperator(Operator.AND)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));

        // Default
        searchResponse = client().prepareSearch().setQuery(QueryBuilders.commonTerms("field1", "the quick brown").cutoffFrequency(3)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.commonTerms("field1", "the huge fox").lowFreqMinimumShouldMatch("2")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.commonTerms("field1", "the lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("3")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.commonTerms("field1", "the lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("4")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));

        searchResponse = client().prepareSearch().setQuery("{ \"common\" : { \"field1\" : { \"query\" : \"the lazy fox brown\", \"cutoff_frequency\" : 1, \"minimum_should_match\" : { \"high_freq\" : 4 } } } }").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));

        // Default
        searchResponse = client().prepareSearch().setQuery(QueryBuilders.commonTerms("field1", "the lazy fox brown").cutoffFrequency(1)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.commonTerms("field1", "the quick brown").cutoffFrequency(3).analyzer("standard")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(3l));
        // standard drops "the" since its a stopword
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("2"));

        // try the same with match query
        searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.OR)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND).analyzer("standard")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(3l));
        // standard drops "the" since its a stopword
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("2"));

        // try the same with multi match query
        searchResponse = client().prepareSearch().setQuery(QueryBuilders.multiMatchQuery("the quick brown", "field1", "field2").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3")); // better score due to different query stats
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("2"));
    }

    @Test
    public void testOmitTermFreqsAndPositions() throws Exception {
        // backwards compat test!
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,omit_term_freq_and_positions=true")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).get();

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox"));


        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchQuery("field2", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        try {
            client().prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).execute().actionGet();
        } catch (SearchPhaseExecutionException e) {
            assertTrue(e.getMessage().endsWith("IllegalStateException[field \"field1\" was indexed without position data; cannot run PhraseQuery (term=quick)]; }"));
        }
    }

    @Test
    public void queryStringAnalyzedWildcard() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryString("value*").analyzeWildcard(true)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(queryString("*ue*").analyzeWildcard(true)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(queryString("*ue_1").analyzeWildcard(true)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(queryString("val*e_1").analyzeWildcard(true)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(queryString("v?l*e?1").analyzeWildcard(true)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testLowercaseExpandedTerms() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryString("VALUE_3~1").lowercaseExpandedTerms(true)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(queryString("VALUE_3~1").lowercaseExpandedTerms(false)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));
        searchResponse = client().prepareSearch().setQuery(queryString("ValUE_*").lowercaseExpandedTerms(true)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(queryString("vAl*E_1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(queryString("[VALUE_1 TO VALUE_3]")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(queryString("[VALUE_1 TO VALUE_3]").lowercaseExpandedTerms(false)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));
    }

    @Test
    public void testDateRangeInQueryString() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        String aMonthAgo = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).minusMonths(1));
        String aMonthFromNow = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).plusMonths(1));

        client().prepareIndex("test", "type", "1").setSource("past", aMonthAgo, "future", aMonthFromNow).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryString("past:[now-2M/d TO now/d]")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(queryString("future:[now/d TO now+2M/d]").lowercaseExpandedTerms(false)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        try {
            client().prepareSearch().setQuery(queryString("future:[now/D TO now+2M/d]").lowercaseExpandedTerms(false)).execute().actionGet();
            fail("D is an unsupported unit in date math");
        } catch (Exception e) {
            // expected
        }
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
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                        .startObject("_type").field("index", index).endObject()
                        .endObject().endObject())
                .addMapping("type2", jsonBuilder().startObject().startObject("type2")
                        .startObject("_type").field("index", index).endObject()
                        .endObject().endObject())
                .execute().actionGet();
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "2").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "3").setSource("field1", "value1"));

        assertThat(client().prepareCount().setQuery(filteredQuery(matchAllQuery(), typeFilter("type1"))).execute().actionGet().getCount(), equalTo(2l));
        assertThat(client().prepareCount().setQuery(filteredQuery(matchAllQuery(), typeFilter("type2"))).execute().actionGet().getCount(), equalTo(3l));

        assertThat(client().prepareCount().setTypes("type1").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(2l));
        assertThat(client().prepareCount().setTypes("type2").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(3l));

        assertThat(client().prepareCount().setTypes("type1", "type2").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(5l));
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
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                        .startObject("_id").field("index", index).endObject()
                        .endObject().endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2").execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource("field1", "value3").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsFilter("type1").ids("1", "3"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        // no type
        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsFilter().ids("1", "3"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1").ids("1", "3")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        // no type
        searchResponse = client().prepareSearch().setQuery(idsQuery().ids("1", "3")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1").ids("7", "10")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        // repeat..., with terms
        searchResponse = client().prepareSearch().setTypes("type1").setQuery(constantScoreQuery(termsFilter("_id", "1", "3"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));
    }

    @Test
    public void testLimitFilter() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value1_2").execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource("field2", "value2_3").execute().actionGet();
        client().prepareIndex("test", "type1", "4").setSource("field3", "value3_4").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), limitFilter(2))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }

    @Test
    public void filterExistsMissingTests() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x1", "x_1").field("field1", "value1_1").field("field2", "value2_1").endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x2", "x_2").field("field1", "value1_2").endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y1", "y_1").field("field2", "value2_3").endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y2", "y_2").field("field3", "value3_4").endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(existsFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client().prepareSearch().setQuery(queryString("_exists_:field1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field2"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field3"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("4"));

        // wildcard check
        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("x*"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));

        // object check
        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("obj1"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));


        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(missingFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        searchResponse = client().prepareSearch().setQuery(queryString("_missing_:field1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        // wildcard check
        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("x*"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        // object check
        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("obj1"))).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));
    }

    @Test
    public void passQueryOrFilterAsJSONStringTest() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefresh(true).execute().actionGet();

        WrapperQueryBuilder wrapper = new WrapperQueryBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }");
        SearchResponse searchResponse = client().prepareSearch().setQuery(wrapper).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        BoolQueryBuilder bool = new BoolQueryBuilder();
        bool.must(wrapper);
        bool.must(new TermQueryBuilder("field2", "value2_1"));

        searchResponse = client().prepareSearch().setQuery(wrapper).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        WrapperFilterBuilder wrapperFilter = new WrapperFilterBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }");

        searchResponse = client().prepareSearch().setFilter(wrapperFilter).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testFiltersWithCustomCacheKey() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testMatchQueryNumeric() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("long", 1l, "double", 1.0d).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("long", 2l, "double", 2.0d).execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource("long", 3l, "double", 3.0d).execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        SearchResponse searchResponse = client().prepareSearch().setQuery(matchQuery("long", "1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

        searchResponse = client().prepareSearch().setQuery(matchQuery("double", "2")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        try {
            searchResponse = client().prepareSearch().setQuery(matchQuery("double", "2 3 4")).execute().actionGet();
            assert false;
        } catch (SearchPhaseExecutionException ex) {
            // number format exception
        }
    }

    @Test
    public void testMultiMatchQuery() throws Exception {

        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value4", "field3", "value3").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2", "field2", "value5", "field3", "value2").execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource("field1", "value3", "field2", "value6", "field3", "value1").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        MultiMatchQueryBuilder builder = QueryBuilders.multiMatchQuery("value1 value2 value4", "field1", "field2");
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(builder)
                .addFacet(FacetBuilders.termsFacet("field1").field("field1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        // this uses dismax so scores are equal and the order can be arbitrary
        assertSearchHits(searchResponse, "1", "2");

        builder.useDisMax(false);
        searchResponse = client().prepareSearch()
                .setQuery(builder)
                .execute().actionGet();

        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat("1", equalTo(searchResponse.getHits().getAt(0).id()));
        assertThat("2", equalTo(searchResponse.getHits().getAt(1).id()));

        client().admin().indices().prepareRefresh("test").execute().actionGet();
        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field2")
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client().prepareSearch()
                .setQuery(builder)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat("1", equalTo(searchResponse.getHits().getAt(0).id()));

        client().admin().indices().prepareRefresh("test").execute().actionGet();
        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field3^1.5")
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client().prepareSearch()
                .setQuery(builder)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat("3", equalTo(searchResponse.getHits().getAt(0).id()));
        assertThat("1", equalTo(searchResponse.getHits().getAt(1).id()));

        client().admin().indices().prepareRefresh("test").execute().actionGet();
        builder = QueryBuilders.multiMatchQuery("value1").field("field1").field("field3", 1.5f)
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client().prepareSearch()
                .setQuery(builder)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat("3", equalTo(searchResponse.getHits().getAt(0).id()));
        assertThat("1", equalTo(searchResponse.getHits().getAt(1).id()));

        // Test lenient
        client().prepareIndex("test", "type1", "3").setSource("field1", "value7", "field2", "value8", "field4", 5).execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field2", "field4");
        try {
            client().prepareSearch()
                    .setQuery(builder)
                    .execute().actionGet();
            fail("Exception expected");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.shardFailures()[0].status(), equalTo(RestStatus.BAD_REQUEST));
        }

        builder.lenient(true);
        searchResponse = client().prepareSearch().setQuery(builder).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat("1", equalTo(searchResponse.getHits().getAt(0).id()));
    }

    @Test
    public void testMatchQueryZeroTermsQuery() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        BoolQueryBuilder boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE));
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }

    @Test
    public void testMultiMatchQueryZeroTermsQuery() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value3", "field2", "value4").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        BoolQueryBuilder boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE))
                .must(multiMatchQuery("value1", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE)); // Fields are ORed together
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL))
                .must(multiMatchQuery("value4", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }

    @Test
    public void testMultiMatchQueryMinShouldMatch() {

        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "type1", "1").setSource("field1", new String[]{"value1", "value2", "value3"}).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value1").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        MultiMatchQueryBuilder multiMatchQuery = multiMatchQuery("value1 value2 foo", "field1", "field2");

        multiMatchQuery.useDisMax(true);
        multiMatchQuery.minimumShouldMatch("70%");
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getHits()[0].id(), equalTo("1"));

        multiMatchQuery.minimumShouldMatch("30%");
        searchResponse = client().prepareSearch()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].id(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].id(), equalTo("2"));

        multiMatchQuery.useDisMax(false);
        multiMatchQuery.minimumShouldMatch("70%");
        searchResponse = client().prepareSearch()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getHits()[0].id(), equalTo("1"));

        multiMatchQuery.minimumShouldMatch("30%");
        searchResponse = client().prepareSearch()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].id(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].id(), equalTo("2"));

        multiMatchQuery = multiMatchQuery("value1 value2 bar", "field1");
        multiMatchQuery.minimumShouldMatch("100%");
        searchResponse = client().prepareSearch()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        multiMatchQuery.minimumShouldMatch("70%");
        searchResponse = client().prepareSearch()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getHits()[0].id(), equalTo("1"));
    }

    @Test
    public void testFuzzyQueryString() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "type1", "1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("str", "shay", "date", "2012-02-05", "num", 20).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(queryString("str:kimcy~1"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

        searchResponse = client().prepareSearch()
                .setQuery(queryString("num:11~1"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

        searchResponse = client().prepareSearch()
                .setQuery(queryString("date:2012-02-02~1d"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testQuotedQueryStringWithBoost() throws InterruptedException, ExecutionException {
        float boost = 10.0f;
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("important", "phrase match", "less_important", "nothing important"),
            client().prepareIndex("test", "type1", "2").setSource("important", "nothing important", "less_important", "phrase match")
        );

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(queryString("\"phrase match\"").field("important", boost).field("less_important"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("2"));
        assertThat((double)searchResponse.getHits().getAt(0).score(), closeTo(boost * searchResponse.getHits().getAt(1).score(), .1));

        searchResponse = client().prepareSearch()
                .setQuery(queryString("\"phrase match\"").field("important", boost).field("less_important").useDisMax(false))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("2"));
        assertThat((double)searchResponse.getHits().getAt(0).score(), closeTo(boost * searchResponse.getHits().getAt(1).score(), .1));
    }
    
    @Test
    public void testSpecialRangeSyntaxInQueryString() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "type1", "1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("str", "shay", "date", "2012-02-05", "num", 20).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(queryString("num:>19"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(queryString("num:>20"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        searchResponse = client().prepareSearch()
                .setQuery(queryString("num:>=20"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(queryString("num:>11"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));

        searchResponse = client().prepareSearch()
                .setQuery(queryString("num:<20"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch()
                .setQuery(queryString("num:<=20"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));

        searchResponse = client().prepareSearch()
                .setQuery(queryString("+num:>11 +num:<20"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testEmptyTermsFilter() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", 
                jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("term").field("type", "string").endObject()
                    .endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("test", "type", "1").setSource("term", "1").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("term", "2").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("term", "3").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("term", "4").execute().actionGet();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("term", new String[0]))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), idsFilter())
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));
    }

    @Test
    public void testTermsLookupFilter() throws Exception {
        assertAcked(prepareCreate("lookup").addMapping("type", 
                jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("terms").field("type", "string").endObject()
                    .startObject("other").field("type", "string").endObject()
                    .endObject().endObject().endObject()));
        assertAcked(prepareCreate("lookup2").addMapping("type", 
                jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("arr").startObject("properties").startObject("term").field("type", "string")
                    .endObject().endObject().endObject().endObject().endObject().endObject()));
        assertAcked(prepareCreate("test").addMapping("type", 
                jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("term").field("type", "string").endObject()
                    .endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("lookup", "type", "1").setSource("terms", new String[]{"1", "3"}).execute().actionGet();
        client().prepareIndex("lookup", "type", "2").setSource("terms", new String[]{"2"}).execute().actionGet();
        client().prepareIndex("lookup", "type", "3").setSource("terms", new String[]{"2", "4"}).execute().actionGet();
        client().prepareIndex("lookup", "type", "4").setSource("other", "value").execute().actionGet();

        client().prepareIndex("lookup2", "type", "1").setSource(XContentFactory.jsonBuilder().startObject()
                .startArray("arr")
                .startObject().field("term", "1").endObject()
                .startObject().field("term", "3").endObject()
                .endArray()
                .endObject()).execute().actionGet();
        client().prepareIndex("lookup2", "type", "2").setSource(XContentFactory.jsonBuilder().startObject()
                .startArray("arr")
                .startObject().field("term", "2").endObject()
                .endArray()
                .endObject()).execute().actionGet();
        client().prepareIndex("lookup2", "type", "3").setSource(XContentFactory.jsonBuilder().startObject()
                .startArray("arr")
                .startObject().field("term", "2").endObject()
                .startObject().field("term", "4").endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type", "1").setSource("term", "1").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("term", "2").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("term", "3").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("term", "4").execute().actionGet();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getHits()[1].getId(), anyOf(equalTo("1"), equalTo("3")));

        // same as above, just on the _id...
        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("_id").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getHits()[1].getId(), anyOf(equalTo("1"), equalTo("3")));

        // another search with same parameters...
        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getHits()[1].getId(), anyOf(equalTo("1"), equalTo("3")));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("2").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("3").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), anyOf(equalTo("2"), equalTo("4")));
        assertThat(searchResponse.getHits().getHits()[1].getId(), anyOf(equalTo("2"), equalTo("4")));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("4").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));


        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("1").lookupPath("arr.term"))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getHits()[1].getId(), anyOf(equalTo("1"), equalTo("3")));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("2").lookupPath("arr.term"))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("3").lookupPath("arr.term"))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), anyOf(equalTo("2"), equalTo("4")));
        assertThat(searchResponse.getHits().getHits()[1].getId(), anyOf(equalTo("2"), equalTo("4")));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("not_exists").lookupIndex("lookup2").lookupType("type").lookupId("3").lookupPath("arr.term"))
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));
    }

    @Test
    public void testBasicFilterById() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type2", "2").setSource(jsonBuilder().startObject()
                .field("field1", "value2")
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setFilter(FilterBuilders.idsFilter("type1").ids("1")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1", "type2").ids("1", "2"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setFilter(FilterBuilders.idsFilter().ids("1")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setFilter(FilterBuilders.idsFilter().ids("1", "2")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter().ids("1", "2"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));


        searchResponse = client().prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1").ids("1", "2"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter().ids("1"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter(null).ids("1"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1", "type2", "type3").ids("1", "2", "3", "4"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
    }

    @Test
    public void testBasicQueryById() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type2", "2").setSource(jsonBuilder().startObject()
                .field("field1", "value2")
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.idsQuery("type1", "type2").ids("1", "2")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.idsQuery().ids("1")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.idsQuery().ids("1", "2")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));


        searchResponse = client().prepareSearch().setQuery(QueryBuilders.idsQuery("type1").ids("1", "2")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.idsQuery().ids("1")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.idsQuery(null).ids("1")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.idsQuery("type1", "type2", "type3").ids("1", "2", "3", "4")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
    }

    @Test
    public void testNumericTermsAndRanges() throws Exception {
                client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("num_byte").field("type", "byte").endObject()
                        .startObject("num_short").field("type", "short").endObject()
                        .startObject("num_integer").field("type", "integer").endObject()
                        .startObject("num_long").field("type", "long").endObject()
                        .startObject("num_float").field("type", "float").endObject()
                        .startObject("num_double").field("type", "double").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("num_byte", 1)
                .field("num_short", 1)
                .field("num_integer", 1)
                .field("num_long", 1)
                .field("num_float", 1)
                .field("num_double", 1)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("num_byte", 2)
                .field("num_short", 2)
                .field("num_integer", 2)
                .field("num_long", 2)
                .field("num_float", 2)
                .field("num_double", 2)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "17").setSource(jsonBuilder().startObject()
                .field("num_byte", 17)
                .field("num_short", 17)
                .field("num_integer", 17)
                .field("num_long", 17)
                .field("num_float", 17)
                .field("num_double", 17)
                .endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse;

        logger.info("--> term query on 1");
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_byte", 1)).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_short", 1)).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_integer", 1)).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_long", 1)).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_float", 1)).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_double", 1)).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        logger.info("--> terms query on 1");
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_byte", new int[]{1})).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_short", new int[]{1})).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_integer", new int[]{1})).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_long", new int[]{1})).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_float", new double[]{1})).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_double", new double[]{1})).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        logger.info("--> term filter on 1");
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_byte", 1))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_short", 1))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_integer", 1))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_long", 1))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_float", 1))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_double", 1))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        logger.info("--> terms filter on 1");
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_byte", new int[]{1}))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_short", new int[]{1}))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_integer", new int[]{1}))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_long", new int[]{1}))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_float", new int[]{1}))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_double", new int[]{1}))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    @Test
    public void testNumericRangeFilter_2826() throws Exception {
                client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
        )
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("num_byte").field("type", "byte").endObject()
                        .startObject("num_short").field("type", "short").endObject()
                        .startObject("num_integer").field("type", "integer").endObject()
                        .startObject("num_long").field("type", "long").endObject()
                        .startObject("num_float").field("type", "float").endObject()
                        .startObject("num_double").field("type", "double").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "test1")
                .field("num_long", 1)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("field1", "test1")
                .field("num_long", 2)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("field1", "test2")
                .field("num_long", 3)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("field1", "test2")
                .field("num_long", 4)
                .endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();
        SearchResponse response = client().prepareSearch("test").setFilter(
                FilterBuilders.boolFilter()
                        .should(FilterBuilders.rangeFilter("num_long").from(1).to(2))
                        .should(FilterBuilders.rangeFilter("num_long").from(3).to(4))
        ).execute().actionGet();
        assertThat(response.getHits().totalHits(), equalTo(4l));

        // This made 2826 fail! (only with bit based filters)
        response = client().prepareSearch("test").setFilter(
                FilterBuilders.boolFilter()
                        .should(FilterBuilders.numericRangeFilter("num_long").from(1).to(2))
                        .should(FilterBuilders.numericRangeFilter("num_long").from(3).to(4))
        ).execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(4l));

        // This made #2979 fail!
        response = client().prepareSearch("test").setFilter(
                FilterBuilders.boolFilter()
                        .must(FilterBuilders.termFilter("field1", "test1"))
                        .should(FilterBuilders.rangeFilter("num_long").from(1).to(2))
                        .should(FilterBuilders.rangeFilter("num_long").from(3).to(4))
        ).execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
    }

    @Test
    public void testEmptyTopLevelFilter() {
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefresh(true).execute().actionGet();
        SearchResponse searchResponse = client().prepareSearch().setFilter("{}").execute().actionGet();
        assertNoFailures(searchResponse);
    }

    @Test // see #2926
    public void testMustNot() throws ElasticSearchException, IOException {
                client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
        )
                .execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource(jsonBuilder().startObject()
                .field("description", "foo other anything bar")
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "test", "2").setSource(jsonBuilder().startObject()
                .field("description", "foo other anything")
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "test", "3").setSource(jsonBuilder().startObject()
                .field("description", "foo other")
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "test", "4").setSource(jsonBuilder().startObject()
                .field("description", "foo")
                .endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .execute().actionGet();
        assertNoFailures(response);
        assertThat(response.getHits().totalHits(), equalTo(4l));

        response = client().prepareSearch("test").setQuery(
                QueryBuilders.boolQuery()
                        .mustNot(QueryBuilders.matchQuery("description", "anything").type(Type.BOOLEAN))
        ).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).execute().actionGet();
        assertThat(response.getShardFailures().length, equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(2l));
    }

    @Test // see #2994
    public void testSimpleSpan() throws ElasticSearchException, IOException {

        client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
        )
                .execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource(jsonBuilder().startObject()
                .field("description", "foo other anything bar")
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "test", "2").setSource(jsonBuilder().startObject()
                .field("description", "foo other anything")
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "test", "3").setSource(jsonBuilder().startObject()
                .field("description", "foo other")
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "test", "4").setSource(jsonBuilder().startObject()
                .field("description", "foo")
                .endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.spanOrQuery().clause(QueryBuilders.spanTermQuery("description", "bar")))
                .execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 1l);
        response = client().prepareSearch("test")
                .setQuery(QueryBuilders.spanOrQuery().clause(QueryBuilders.spanTermQuery("test.description", "bar")))
                .execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 1l);

        response = client().prepareSearch("test").setQuery(
                QueryBuilders.spanNearQuery()
                        .clause(QueryBuilders.spanTermQuery("description", "foo"))
                        .clause(QueryBuilders.spanTermQuery("test.description", "other"))
                        .slop(3)).execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 3l);
    }

    @Test
    public void testSpanMultiTermQuery() throws ElasticSearchException, IOException {

        client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
        )
                .execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource(jsonBuilder().startObject()
                .field("description", "foo other anything bar")
                .field("count", 1)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "test", "2").setSource(jsonBuilder().startObject()
                .field("description", "foo other anything")
                .field("count", 2)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "test", "3").setSource(jsonBuilder().startObject()
                .field("description", "foo other")
                .field("count", 3)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "test", "4").setSource(jsonBuilder().startObject()
                .field("description", "fop")
                .field("count", 4)
                .endObject())
                .execute().actionGet();

        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.spanOrQuery().clause(QueryBuilders.spanMultiTermQueryBuilder(QueryBuilders.fuzzyQuery("description", "fop"))))
                .execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 4);

        response = client().prepareSearch("test")
                .setQuery(QueryBuilders.spanOrQuery().clause(QueryBuilders.spanMultiTermQueryBuilder(QueryBuilders.prefixQuery("description", "fo"))))
                .execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 4);

        response = client().prepareSearch("test")
                .setQuery(QueryBuilders.spanOrQuery().clause(QueryBuilders.spanMultiTermQueryBuilder(QueryBuilders.wildcardQuery("description", "oth*"))))
                .execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 3);

        response = client().prepareSearch("test")
                .setQuery(QueryBuilders.spanOrQuery().clause(QueryBuilders.spanMultiTermQueryBuilder(QueryBuilders.rangeQuery("description").from("ffa").to("foo"))))
                .execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 3);

        response = client().prepareSearch("test")
                .setQuery(QueryBuilders.spanOrQuery().clause(QueryBuilders.spanMultiTermQueryBuilder(QueryBuilders.regexpQuery("description", "fo{2}"))))
                .execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 3);
    }

    @Test
    public void testSimpleDFSQuery() throws ElasticSearchException, IOException {
        prepareCreate("test", -1,
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 5)
                        .put("index.number_of_replicas", 0)
        ).addMapping("s", jsonBuilder()
                .startObject()
                    .startObject("s")
                        .startObject("_routing")
                            .field("required", true)
                            .field("path", "bs")
                        .endObject()
                        .startObject("properties")
                            .startObject("online")
                                .field("type", "boolean")
                            .endObject()
                            .startObject("ts")
                                .field("type", "date")
                                .field("ignore_malformed", false)
                                .field("format", "dateOptionalTime")
                            .endObject()
                            .startObject("bs")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject())
                .addMapping("bs", jsonBuilder()
                        .startObject()
                            .startObject("s")
                                .startObject("properties")
                                    .startObject("online")
                                        .field("type", "boolean")
                                    .endObject()
                                    .startObject("ts")
                                        .field("type", "date")
                                        .field("ignore_malformed", false)
                                        .field("format", "dateOptionalTime")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject())
                .execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "s", "1").setSource(jsonBuilder().startObject()
                .field("online", false)
                .field("bs", "Y")
                .field("ts", System.currentTimeMillis() - 100)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "s", "2").setSource(jsonBuilder().startObject()
                .field("online", true)
                .field("bs", "X")
                .field("ts", System.currentTimeMillis() - 10000000)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "bs", "3").setSource(jsonBuilder().startObject()
                .field("online", false)
                .field("ts", System.currentTimeMillis() - 100)
                .endObject())
                .execute().actionGet();

        client().prepareIndex("test", "bs", "4").setSource(jsonBuilder().startObject()
                .field("online", true)
                .field("ts", System.currentTimeMillis() - 123123)
                .endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery("online", true))
                                .must(QueryBuilders.boolQuery()
                                        .should(QueryBuilders.boolQuery()
                                                .must(QueryBuilders.rangeQuery("ts").lt(System.currentTimeMillis() - (15 * 1000)))
                                                .must(QueryBuilders.termQuery("_type", "bs"))
                                        )
                                        .should(QueryBuilders.boolQuery()
                                                .must(QueryBuilders.rangeQuery("ts").lt(System.currentTimeMillis() - (15 * 1000)))
                                                .must(QueryBuilders.termQuery("_type", "s"))
                                        )
                                )
                )
                .setVersion(true)
                .setFrom(0).setSize(100).setExplain(true)
                .execute()
                .actionGet();
        assertNoFailures(response);
    }

    @Test
    public void testMultiFieldQueryString() {
        client().prepareIndex("test", "s", "1").setSource("field1", "value1", "field2", "value2").setRefresh(true).get();
        logger.info("regular");
        assertHitCount(client().prepareSearch("test").setQuery(queryString("value1").field("field1").field("field2")).get(), 1);
        assertHitCount(client().prepareSearch("test").setQuery(queryString("field\\*:value1")).get(), 1);
        logger.info("prefix");
        assertHitCount(client().prepareSearch("test").setQuery(queryString("value*").field("field1").field("field2")).get(), 1);
        assertHitCount(client().prepareSearch("test").setQuery(queryString("field\\*:value*")).get(), 1);
        logger.info("wildcard");
        assertHitCount(client().prepareSearch("test").setQuery(queryString("v?lue*").field("field1").field("field2")).get(), 1);
        assertHitCount(client().prepareSearch("test").setQuery(queryString("field\\*:v?lue*")).get(), 1);
        logger.info("fuzzy");
        assertHitCount(client().prepareSearch("test").setQuery(queryString("value~").field("field1").field("field2")).get(), 1);
        assertHitCount(client().prepareSearch("test").setQuery(queryString("field\\*:value~")).get(), 1);
        logger.info("regexp");
        assertHitCount(client().prepareSearch("test").setQuery(queryString("/value[01]/").field("field1").field("field2")).get(), 1);
        assertHitCount(client().prepareSearch("test").setQuery(queryString("field\\*:/value[01]/")).get(), 1);
    }
    
    // see #3881 - for extensive description of the issue
    @Test
    public void testMatchQueryWithSynonyms() throws IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.analysis.analyzer.index.type", "custom")
                .put("index.analysis.analyzer.index.tokenizer", "standard")
                .put("index.analysis.analyzer.index.filter", "lowercase")
                .put("index.analysis.analyzer.search.type", "custom")
                                .put("index.analysis.analyzer.search.tokenizer", "standard")

                .putArray("index.analysis.analyzer.search.filter", "lowercase", "synonym")
                .put("index.analysis.filter.synonym.type", "synonym")
                .putArray("index.analysis.filter.synonym.synonyms", "fast, quick"));
        
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("test")
                .startObject("properties")
                .startObject("text")
                        .field("type", "string")
                        .field("index_analyzer", "index")
                        .field("search_analyzer", "search")
                .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(builder.addMapping("test", mapping));
        ensureGreen();
        client().prepareIndex("test", "test", "1").setSource(jsonBuilder().startObject()
                .field("text", "quick brown fox")
                .endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("text", "quick").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("text", "quick brown").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("text", "fast").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        
        client().prepareIndex("test", "test", "2").setSource(jsonBuilder().startObject()
                .field("text", "fast brown fox")
                .endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("text", "quick").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 2);
        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("text", "quick brown").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 2);
    }
    
    @Test
    public void testMatchQueryWithStackedStems() throws IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.analysis.analyzer.index.type", "custom")
                .put("index.analysis.analyzer.index.tokenizer", "standard")
                .put("index.analysis.analyzer.index.filter", "lowercase")
                .put("index.analysis.analyzer.search.type", "custom")
                .put("index.analysis.analyzer.search.tokenizer", "standard")
                .putArray("index.analysis.analyzer.search.filter", "lowercase", "keyword_repeat", "porterStem", "unique_stem")
                .put("index.analysis.filter.unique_stem.type", "unique")
                .put("index.analysis.filter.unique_stem.only_on_same_position", true));
        
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("test")
                .startObject("properties")
                .startObject("text")
                        .field("type", "string")
                        .field("index_analyzer", "index")
                        .field("search_analyzer", "search")
                .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(builder.addMapping("test", mapping));
        ensureGreen();
        client().prepareIndex("test", "test", "1").setSource(jsonBuilder().startObject()
                .field("text", "the fox runs across the street")
                .endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("text", "fox runs").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        
        client().prepareIndex("test", "test", "2").setSource(jsonBuilder().startObject()
                .field("text", "run fox run")
                .endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("text", "fox runs").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 2);
    }
    
    @Test
    public void testQueryStringWithSynonyms() throws IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.analysis.analyzer.index.type", "custom")
                .put("index.analysis.analyzer.index.tokenizer", "standard")
                .put("index.analysis.analyzer.index.filter", "lowercase")
                .put("index.analysis.analyzer.search.type", "custom")
                                .put("index.analysis.analyzer.search.tokenizer", "standard")

                .putArray("index.analysis.analyzer.search.filter", "lowercase", "synonym")
                .put("index.analysis.filter.synonym.type", "synonym")
                .putArray("index.analysis.filter.synonym.synonyms", "fast, quick"));
        
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("test")
                .startObject("properties")
                .startObject("text")
                        .field("type", "string")
                        .field("index_analyzer", "index")
                        .field("search_analyzer", "search")
                .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(builder.addMapping("test", mapping));
        ensureGreen();
        client().prepareIndex("test", "test", "1").setSource(jsonBuilder().startObject()
                .field("text", "quick brown fox")
                .endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryString("quick").defaultField("text").defaultOperator(QueryStringQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryString("quick brown").defaultField("text").defaultOperator(QueryStringQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch().setQuery(QueryBuilders.queryString("fast").defaultField("text").defaultOperator(QueryStringQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        
        client().prepareIndex("test", "test", "2").setSource(jsonBuilder().startObject()
                .field("text", "fast brown fox")
                .endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryString("quick").defaultField("text").defaultOperator(QueryStringQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 2);
        searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryString("quick brown").defaultField("text").defaultOperator(QueryStringQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 2);
    }

    @Test // see https://github.com/elasticsearch/elasticsearch/issues/3898
    public void testCustomWordDelimiterQueryString() {
        client().admin().indices().prepareCreate("test")
                .setSettings("analysis.analyzer.my_analyzer.type", "custom",
                        "analysis.analyzer.my_analyzer.tokenizer", "whitespace",
                        "analysis.analyzer.my_analyzer.filter", "custom_word_delimiter",
                        "analysis.filter.custom_word_delimiter.type", "word_delimiter",
                        "analysis.filter.custom_word_delimiter.generate_word_parts", "true",
                        "analysis.filter.custom_word_delimiter.generate_number_parts", "false",
                        "analysis.filter.custom_word_delimiter.catenate_numbers", "true",
                        "analysis.filter.custom_word_delimiter.catenate_words", "false",
                        "analysis.filter.custom_word_delimiter.split_on_case_change", "false",
                        "analysis.filter.custom_word_delimiter.split_on_numerics", "false",
                        "analysis.filter.custom_word_delimiter.stem_english_possessive", "false")
                .addMapping("type1", "field1", "type=string,analyzer=my_analyzer", "field2", "type=string,analyzer=my_analyzer")
                .get();

        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "foo bar baz", "field2", "not needed").get();
        refresh();

        SearchResponse response = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.queryString("foo.baz").useDisMax(false).defaultOperator(QueryStringQueryBuilder.Operator.AND)
                                .field("field1").field("field2")).get();

        assertHitCount(response, 1l);
    }

    @Test // see https://github.com/elasticsearch/elasticsearch/issues/3797
    public void testMultiMatchLenientIssue3797() {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("field1", 123, "field2", "value2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.multiMatchQuery("value2", "field1^2", "field2").lenient(true).useDisMax(false)).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.multiMatchQuery("value2", "field1^2", "field2").lenient(true).useDisMax(true)).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.multiMatchQuery("value2", "field2^2").lenient(true)).get();
        assertHitCount(searchResponse, 1l);
    }
}
