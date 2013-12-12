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
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.CommonTermsQueryBuilder.Operator;
import org.elasticsearch.index.query.MatchQueryBuilder.Type;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
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
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;


public class SimpleQueryTests extends ElasticsearchIntegrationTest {

    @Test // see https://github.com/elasticsearch/elasticsearch/issues/3177
    public void testIssue3177() {
        assertAcked(prepareCreate("test").setSettings(settingsBuilder().put(SETTING_NUMBER_OF_SHARDS, 1)));
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2").get();
        client().prepareIndex("test", "type1", "3").setSource("field1", "value3").get();
        ensureGreen();
        waitForRelocation();
        optimize();
        refresh();
        assertHitCount(
                client().prepareSearch()
                        .setQuery(matchAllQuery())
                        .setFilter(
                                andFilter(
                                        queryFilter(matchAllQuery()),
                                        notFilter(andFilter(queryFilter(termQuery("field1", "value1")),
                                                queryFilter(termQuery("field1", "value2")))))).get(),
                3l);
        assertHitCount(
                client().prepareSearch()
                        .setQuery(
                                filteredQuery(
                                        boolQuery().should(termQuery("field1", "value1")).should(termQuery("field1", "value2"))
                                                .should(termQuery("field1", "value3")),
                                        notFilter(andFilter(queryFilter(termQuery("field1", "value1")),
                                                queryFilter(termQuery("field1", "value2")))))).get(),
                3l);
        assertHitCount(
                client().prepareSearch().setQuery(matchAllQuery()).setFilter(notFilter(termFilter("field1", "value3"))).get(), 
                2l);
    }

    @Test
    public void passQueryAsStringTest() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefresh(true).get();

        SearchResponse searchResponse = client().prepareSearch().setQuery("{ \"term\" : { \"field1\" : \"value1_1\" }}").get();
        assertHitCount(searchResponse, 1l);
    }

    @Test
    public void testIndexOptions() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,index_options=docs")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox").setRefresh(true).get();

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchQuery("field2", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).get();
        assertHitCount(searchResponse, 1l);
        try {
            client().prepareSearch().setQuery(matchQuery("field1", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).get();
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
        SearchResponse searchResponse = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("field1", "quick"))).get();
        assertHitCount(searchResponse, 2l);
        for (SearchHit searchHit : searchResponse.getHits().hits()) {
            assertSearchHit(searchHit, hasScore(1.0f));
        }

        searchResponse = client().prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).must(
                constantScoreQuery(matchQuery("field1", "quick")).boost(1.0f + getRandom().nextFloat()))).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasScore(searchResponse.getHits().getAt(1).score()));
        
        client().prepareSearch("test").setQuery(constantScoreQuery(matchQuery("field1", "quick")).boost(1.0f + getRandom().nextFloat())).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasScore(searchResponse.getHits().getAt(1).score()));
        
        searchResponse = client().prepareSearch("test").setQuery(
                constantScoreQuery(boolQuery().must(matchAllQuery()).must(
                constantScoreQuery(matchQuery("field1", "quick")).boost(1.0f + (random.nextBoolean()? 0.0f : random.nextFloat()))))).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasScore(searchResponse.getHits().getAt(1).score()));
        for (SearchHit searchHit : searchResponse.getHits().hits()) {
            assertSearchHit(searchHit, hasScore(1.0f));
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
            MatchQueryBuilder matchQuery = matchQuery("f", English.intToEnglish(between(0, num)));
            searchResponse = client().prepareSearch("test_1").setQuery(matchQuery).setSize(num).get();
            long totalHits = searchResponse.getHits().totalHits();
            SearchHits hits = searchResponse.getHits();
            for (SearchHit searchHit : hits) {
                assertSearchHit(searchHit, hasScore(1.0f));
            }
            if (random.nextBoolean()) {
                searchResponse = client().prepareSearch("test_1").setQuery(
                        boolQuery().must(matchAllQuery()).must(
                        constantScoreQuery(matchQuery).boost(1.0f + (random.nextBoolean()? 0.0f : random.nextFloat())))).setSize(num).get();
                hits = searchResponse.getHits();
            } else {
                FilterBuilder filter = queryFilter(matchQuery);
                searchResponse = client().prepareSearch("test_1").setQuery(
                        boolQuery().must(matchAllQuery()).must(
                        constantScoreQuery(filter).boost(1.0f + (random.nextBoolean()? 0.0f : random.nextFloat())))).setSize(num).get();
                hits = searchResponse.getHits();
            }
            assertThat(hits.totalHits(), equalTo(totalHits));
            if (totalHits > 1) {
                float expected = hits.getAt(0).score();
                for (SearchHit searchHit : hits) {
                    assertSearchHit(searchHit, hasScore(expected));
                }
            }
        }
    }

    @Test // see #3521
    public void testAllDocsQueryString() throws InterruptedException, ExecutionException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_REPLICAS, 0));
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("foo", "bar"),
                client().prepareIndex("test", "type1", "2").setSource("foo", "bar")
        );
        int iters = atLeast(100);
        for (int i = 0; i < iters; i++) {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(queryString("*:*^10.0").boost(10.0f)).get();
            assertHitCount(searchResponse, 2l);
            
            searchResponse = client().prepareSearch("test").setQuery(
                    boolQuery().must(matchAllQuery()).must(constantScoreQuery(matchAllQuery()))).get();
            assertHitCount(searchResponse, 2l);
            assertThat((double)searchResponse.getHits().getAt(0).score(), closeTo(Math.sqrt(2), 0.1));
            assertThat((double)searchResponse.getHits().getAt(1).score(),closeTo(Math.sqrt(2), 0.1));
        }
    }

    @Test
    public void testCommonTermsQuery() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,analyzer=whitespace")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1).get();
        indexRandom(true, client().prepareIndex("test", "type1", "3").setSource("field1", "quick lazy huge brown pidgin", "field2", "the quick lazy huge brown fox jumps over the tree"),
                client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "the quick lazy huge brown fox jumps over the tree") );

        SearchResponse searchResponse = client().prepareSearch().setQuery(commonTerms("field1", "the quick brown").cutoffFrequency(3).lowFreqOperator(Operator.OR)).get();
        assertHitCount(searchResponse, 3l);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(commonTerms("field1", "the quick brown").cutoffFrequency(3).lowFreqOperator(Operator.AND)).get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        // Default
        searchResponse = client().prepareSearch().setQuery(commonTerms("field1", "the quick brown").cutoffFrequency(3)).get();
        assertHitCount(searchResponse, 3l);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));


        searchResponse = client().prepareSearch().setQuery(commonTerms("field1", "the huge fox").lowFreqMinimumShouldMatch("2")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(commonTerms("field1", "the lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("3")).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(commonTerms("field1", "the lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("4")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery("{ \"common\" : { \"field1\" : { \"query\" : \"the lazy fox brown\", \"cutoff_frequency\" : 1, \"minimum_should_match\" : { \"high_freq\" : 4 } } } }").get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        // Default
        searchResponse = client().prepareSearch().setQuery(commonTerms("field1", "the lazy fox brown").cutoffFrequency(1)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(commonTerms("field1", "the quick brown").cutoffFrequency(3).analyzer("standard")).get();
        assertHitCount(searchResponse, 3l);
        // standard drops "the" since its a stopword
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("3"));
        assertThirdHit(searchResponse, hasId("2"));

        // try the same with match query
        searchResponse = client().prepareSearch().setQuery(matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.OR)).get();
        assertHitCount(searchResponse, 3l);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND).analyzer("standard")).get();
        assertHitCount(searchResponse, 3l);
        // standard drops "the" since its a stopword
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("3"));
        assertThirdHit(searchResponse, hasId("2"));

        // try the same with multi match query
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery("the quick brown", "field1", "field2").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 3l);
        assertFirstHit(searchResponse, hasId("3")); // better score due to different query stats
        assertSecondHit(searchResponse, hasId("1"));
        assertThirdHit(searchResponse, hasId("2"));
    }

    @Test
    public void testOmitTermFreqsAndPositions() throws Exception {
        // backwards compat test!
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,omit_term_freq_and_positions=true")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox"));

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchQuery("field2", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).get();
        assertHitCount(searchResponse, 1l);
        try {
            client().prepareSearch().setQuery(matchQuery("field1", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).get();
            fail("SearchPhaseExecutionException should have been thrown");
        } catch (SearchPhaseExecutionException e) {
            assertTrue(e.getMessage().endsWith("IllegalStateException[field \"field1\" was indexed without position data; cannot run PhraseQuery (term=quick)]; }"));
        }
    }

    @Test
    public void queryStringAnalyzedWildcard() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        client().prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryString("value*").analyzeWildcard(true)).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(queryString("*ue*").analyzeWildcard(true)).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(queryString("*ue_1").analyzeWildcard(true)).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(queryString("val*e_1").analyzeWildcard(true)).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(queryString("v?l*e?1").analyzeWildcard(true)).get();
        assertHitCount(searchResponse, 1l);
    }

    @Test
    public void testLowercaseExpandedTerms() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        client().prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryString("VALUE_3~1").lowercaseExpandedTerms(true)).get();
        assertHitCount(searchResponse, 1l);
        searchResponse = client().prepareSearch().setQuery(queryString("VALUE_3~1").lowercaseExpandedTerms(false)).get();
        assertHitCount(searchResponse, 0l);
        searchResponse = client().prepareSearch().setQuery(queryString("ValUE_*").lowercaseExpandedTerms(true)).get();
        assertHitCount(searchResponse, 1l);
        searchResponse = client().prepareSearch().setQuery(queryString("vAl*E_1")).get();
        assertHitCount(searchResponse, 1l);
        searchResponse = client().prepareSearch().setQuery(queryString("[VALUE_1 TO VALUE_3]")).get();
        assertHitCount(searchResponse, 1l);
        searchResponse = client().prepareSearch().setQuery(queryString("[VALUE_1 TO VALUE_3]").lowercaseExpandedTerms(false)).get();
        assertHitCount(searchResponse, 0l);
    }

    @Test
    public void testDateRangeInQueryString() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        String aMonthAgo = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).minusMonths(1));
        String aMonthFromNow = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).plusMonths(1));
        client().prepareIndex("test", "type", "1").setSource("past", aMonthAgo, "future", aMonthFromNow).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryString("past:[now-2M/d TO now/d]")).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(queryString("future:[now/d TO now+2M/d]").lowercaseExpandedTerms(false)).get();
        assertHitCount(searchResponse, 1l);

        try {
            client().prepareSearch().setQuery(queryString("future:[now/D TO now+2M/d]").lowercaseExpandedTerms(false)).get();
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
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1)
                .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                        .startObject("_type").field("index", index).endObject()
                        .endObject().endObject())
                .addMapping("type2", jsonBuilder().startObject().startObject("type2")
                        .startObject("_type").field("index", index).endObject()
                        .endObject().endObject()));
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "2").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "3").setSource("field1", "value1"));

        assertHitCount(client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), typeFilter("type1"))).get(), 2l);
        assertHitCount(client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), typeFilter("type2"))).get(), 3l);

        assertHitCount(client().prepareSearch().setTypes("type1").setQuery(matchAllQuery()).get(), 2l);
        assertHitCount(client().prepareSearch().setTypes("type2").setQuery(matchAllQuery()).get(), 3l);

        assertHitCount(client().prepareSearch().setTypes("type1", "type2").setQuery(matchAllQuery()).get(), 5l);
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
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1)
                .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                        .startObject("_id").field("index", index).endObject()
                        .endObject().endObject()));

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value2"),
                client().prepareIndex("test", "type1", "3").setSource("field1", "value3"));

        SearchResponse searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsFilter("type1").ids("1", "3"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");

        // no type
        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsFilter().ids("1", "3"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1").ids("1", "3")).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");

        // no type
        searchResponse = client().prepareSearch().setQuery(idsQuery().ids("1", "3")).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1").ids("7", "10")).get();
        assertHitCount(searchResponse, 0l);

        // repeat..., with terms
        searchResponse = client().prepareSearch().setTypes("type1").setQuery(constantScoreQuery(termsFilter("_id", "1", "3"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");
    }

    @Test
    public void testLimitFilter() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value1_2"),
                client().prepareIndex("test", "type1", "3").setSource("field2", "value2_3"),
                client().prepareIndex("test", "type1", "4").setSource("field3", "value3_4"));

        assertHitCount(client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), limitFilter(2))).get(), 2l);
    }

    @Test
    public void filterExistsMissingTests() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        indexRandom(true,
                client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x1", "x_1").field("field1", "value1_1").field("field2", "value2_1").endObject()),
                client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x2", "x_2").field("field1", "value1_2").endObject()),
                client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y1", "y_1").field("field2", "value2_3").endObject()),
                client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y2", "y_2").field("field3", "value3_4").endObject()) );

        SearchResponse searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field1"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "2");

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(existsFilter("field1"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "2");

        searchResponse = client().prepareSearch().setQuery(queryString("_exists_:field1")).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "2");

        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field2"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field3"))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("4"));

        // wildcard check
        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("x*"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "2");

        // object check
        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("obj1"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "2");

        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "4");

        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "4");

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(missingFilter("field1"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "4");

        searchResponse = client().prepareSearch().setQuery(queryString("_missing_:field1")).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "4");

        // wildcard check
        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("x*"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "4");

        // object check
        searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("obj1"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "4");
    }

    @Test
    public void passQueryOrFilterAsJSONStringTest() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefresh(true).get();

        WrapperQueryBuilder wrapper = new WrapperQueryBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }");
        assertHitCount(client().prepareSearch().setQuery(wrapper).get(), 1l);

        BoolQueryBuilder bool = boolQuery().must(wrapper).must(new TermQueryBuilder("field2", "value2_1"));
        assertHitCount(client().prepareSearch().setQuery(bool).get(), 1l);

        WrapperFilterBuilder wrapperFilter = new WrapperFilterBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }");
        assertHitCount(client().prepareSearch().setFilter(wrapperFilter).get(), 1l);
    }

    @Test
    public void testFiltersWithCustomCacheKey() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1"))).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1"))).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1"))).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsFilter("field1", "value1"))).get();
        assertHitCount(searchResponse, 1l);
    }

    @Test
    public void testMatchQueryNumeric() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("long", 1l, "double", 1.0d),
                client().prepareIndex("test", "type1", "2").setSource("long", 2l, "double", 2.0d),
                client().prepareIndex("test", "type1", "3").setSource("long", 3l, "double", 3.0d));

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchQuery("long", "1")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));

        searchResponse = client().prepareSearch().setQuery(matchQuery("double", "2")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));
        try {
            client().prepareSearch().setQuery(matchQuery("double", "2 3 4")).get();
            fail("SearchPhaseExecutionException should have been thrown");
        } catch (SearchPhaseExecutionException ex) {
            // number format exception
        }
    }

    @Test
    public void testMultiMatchQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        indexRandom(true,
                client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value4", "field3", "value3"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value2", "field2", "value5", "field3", "value2"),
                client().prepareIndex("test", "type1", "3").setSource("field1", "value3", "field2", "value6", "field3", "value1") );

        MultiMatchQueryBuilder builder = multiMatchQuery("value1 value2 value4", "field1", "field2");
        SearchResponse searchResponse = client().prepareSearch().setQuery(builder)
                .addFacet(FacetBuilders.termsFacet("field1").field("field1")).get();

        assertHitCount(searchResponse, 2l);
        // this uses dismax so scores are equal and the order can be arbitrary
        assertSearchHits(searchResponse, "1", "2");

        builder.useDisMax(false);
        searchResponse = client().prepareSearch()
                .setQuery(builder)
                .get();

        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "2");

        client().admin().indices().prepareRefresh("test").get();
        builder = multiMatchQuery("value1", "field1", "field2")
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client().prepareSearch()
                .setQuery(builder)
                .get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));

        refresh();
        builder = multiMatchQuery("value1", "field1", "field3^1.5")
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client().prepareSearch().setQuery(builder).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "1");

        client().admin().indices().prepareRefresh("test").get();
        builder = multiMatchQuery("value1").field("field1").field("field3", 1.5f)
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client().prepareSearch().setQuery(builder).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "1");

        // Test lenient
        client().prepareIndex("test", "type1", "3").setSource("field1", "value7", "field2", "value8", "field4", 5).get();
        refresh();

        builder = multiMatchQuery("value1", "field1", "field2", "field4");
        try {
            client().prepareSearch().setQuery(builder).get();
            fail("Exception expected");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.shardFailures()[0].status(), equalTo(RestStatus.BAD_REQUEST));
        }

        builder.lenient(true);
        searchResponse = client().prepareSearch().setQuery(builder).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
    }

    @Test
    public void testMatchQueryZeroTermsQuery() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2").get();
        refresh();

        BoolQueryBuilder boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE));
        SearchResponse searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 0l);

        boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 1l);

        boolQuery = boolQuery().must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 2l);
    }

    public void testMultiMatchQueryZeroTermsQuery() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value3", "field2", "value4").get();
        refresh();

        BoolQueryBuilder boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE))
                .must(multiMatchQuery("value1", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE)); // Fields are ORed together
        SearchResponse searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 0l);

        boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL))
                .must(multiMatchQuery("value4", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 1l);

        boolQuery = boolQuery().must(multiMatchQuery("a", "field1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 2l);
    }

    @Test
    public void testMultiMatchQueryMinShouldMatch() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));
        client().prepareIndex("test", "type1", "1").setSource("field1", new String[]{"value1", "value2", "value3"}).get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value1").get();
        refresh();

        MultiMatchQueryBuilder multiMatchQuery = multiMatchQuery("value1 value2 foo", "field1", "field2");

        multiMatchQuery.useDisMax(true);
        multiMatchQuery.minimumShouldMatch("70%");
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(multiMatchQuery)
                .get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));

        multiMatchQuery.minimumShouldMatch("30%");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        multiMatchQuery.useDisMax(false);
        multiMatchQuery.minimumShouldMatch("70%");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));

        multiMatchQuery.minimumShouldMatch("30%");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        multiMatchQuery = multiMatchQuery("value1 value2 bar", "field1");
        multiMatchQuery.minimumShouldMatch("100%");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 0l);

        multiMatchQuery.minimumShouldMatch("70%");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
    }

    @Test
    public void testFuzzyQueryString() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));
        client().prepareIndex("test", "type1", "1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).get();
        client().prepareIndex("test", "type1", "2").setSource("str", "shay", "date", "2012-02-05", "num", 20).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryString("str:kimcy~1")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));

        searchResponse = client().prepareSearch().setQuery(queryString("num:11~1")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));

        searchResponse = client().prepareSearch().setQuery(queryString("date:2012-02-02~1d")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
    }

    @Test
    public void testQuotedQueryStringWithBoost() throws InterruptedException, ExecutionException {
        float boost = 10.0f;
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("important", "phrase match", "less_important", "nothing important"),
                client().prepareIndex("test", "type1", "2").setSource("important", "nothing important", "less_important", "phrase match")
        );

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(queryString("\"phrase match\"").field("important", boost).field("less_important")).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThat((double)searchResponse.getHits().getAt(0).score(), closeTo(boost * searchResponse.getHits().getAt(1).score(), .1));

        searchResponse = client().prepareSearch()
                .setQuery(queryString("\"phrase match\"").field("important", boost).field("less_important").useDisMax(false)).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThat((double)searchResponse.getHits().getAt(0).score(), closeTo(boost * searchResponse.getHits().getAt(1).score(), .1));
    }

    @Test
    public void testSpecialRangeSyntaxInQueryString() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));
        client().prepareIndex("test", "type1", "1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).get();
        client().prepareIndex("test", "type1", "2").setSource("str", "shay", "date", "2012-02-05", "num", 20).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryString("num:>19")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(queryString("num:>20")).get();
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch().setQuery(queryString("num:>=20")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(queryString("num:>11")).get();
        assertHitCount(searchResponse, 2l);

        searchResponse = client().prepareSearch().setQuery(queryString("num:<20")).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(queryString("num:<=20")).get();
        assertHitCount(searchResponse, 2l);

        searchResponse = client().prepareSearch().setQuery(queryString("+num:>11 +num:<20")).get();
        assertHitCount(searchResponse, 1l);
    }

    @Test
    public void testEmptyTermsFilter() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", "term", "type=string"));
        ensureGreen();
        indexRandom(true, client().prepareIndex("test", "type", "1").setSource("term", "1"),
                client().prepareIndex("test", "type", "2").setSource("term", "2"),
                client().prepareIndex("test", "type", "3").setSource("term", "3"),
                client().prepareIndex("test", "type", "4").setSource("term", "4") );

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("term", new String[0]))).get();
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), idsFilter())).get();
        assertHitCount(searchResponse, 0l);
    }

    @Test
    public void testFieldDataTermsFilter() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", "str", "type=string", "lng", "type=long", "dbl", "type=double"));
        ensureGreen();
        client().prepareIndex("test", "type", "1").setSource("str", "1", "lng", 1l, "dbl", 1.0d).get();
        client().prepareIndex("test", "type", "2").setSource("str", "2", "lng", 2l, "dbl", 2.0d).get();
        client().prepareIndex("test", "type", "3").setSource("str", "3", "lng", 3l, "dbl", 3.0d).get();
        client().prepareIndex("test", "type", "4").setSource("str", "4", "lng", 4l, "dbl", 4.0d).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("str", "1", "4").execution("fielddata"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "4");

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("lng", new long[] {2, 3}).execution("fielddata"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "2", "3");

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("dbl", new double[]{2, 3}).execution("fielddata"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "2", "3");

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("lng", new int[] {1, 3}).execution("fielddata"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("dbl", new float[] {2, 4}).execution("fielddata"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "2", "4");

        // test partial matching
        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("str", "2", "5").execution("fielddata"))).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("dbl", new double[] {2, 5}).execution("fielddata"))).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("lng", new long[] {2, 5}).execution("fielddata"))).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        // test valid type, but no matching terms
        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("str", "5", "6").execution("fielddata"))).get();
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("dbl", new double[] {5, 6}).execution("fielddata"))).get();
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("lng", new long[] {5, 6}).execution("fielddata"))).get();
        assertHitCount(searchResponse, 0l);
    }

    @Test
    public void testTermsLookupFilter() throws Exception {
        assertAcked(prepareCreate("lookup").addMapping("type", "terms","type=string", "other", "type=string"));
        assertAcked(prepareCreate("lookup2").addMapping("type",
                jsonBuilder().startObject().startObject("type").startObject("properties")
                        .startObject("arr").startObject("properties").startObject("term").field("type", "string")
                        .endObject().endObject().endObject().endObject().endObject().endObject()));
        assertAcked(prepareCreate("test").addMapping("type", "term", "type=string"));

        ensureGreen();

        indexRandom(true,
                client().prepareIndex("lookup", "type", "1").setSource("terms", new String[]{"1", "3"}),
                client().prepareIndex("lookup", "type", "2").setSource("terms", new String[]{"2"}),
                client().prepareIndex("lookup", "type", "3").setSource("terms", new String[]{"2", "4"}),
                client().prepareIndex("lookup", "type", "4").setSource("other", "value"),
                client().prepareIndex("lookup2", "type", "1").setSource(XContentFactory.jsonBuilder().startObject()
                        .startArray("arr")
                        .startObject().field("term", "1").endObject()
                        .startObject().field("term", "3").endObject()
                        .endArray()
                        .endObject()),
                client().prepareIndex("lookup2", "type", "2").setSource(XContentFactory.jsonBuilder().startObject()
                        .startArray("arr")
                        .startObject().field("term", "2").endObject()
                        .endArray()
                        .endObject()),
                client().prepareIndex("lookup2", "type", "3").setSource(XContentFactory.jsonBuilder().startObject()
                        .startArray("arr")
                        .startObject().field("term", "2").endObject()
                        .startObject().field("term", "4").endObject()
                        .endArray()
                        .endObject()),
                client().prepareIndex("test", "type", "1").setSource("term", "1"),
                client().prepareIndex("test", "type", "2").setSource("term", "2"),
                client().prepareIndex("test", "type", "3").setSource("term", "3"),
                client().prepareIndex("test", "type", "4").setSource("term", "4") );

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))
                ).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");

        // same as above, just on the _id...
        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("_id").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))
                ).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");

        // another search with same parameters...
        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))
                ).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("2").lookupPath("terms"))
                ).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("3").lookupPath("terms"))
                ).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "2", "4");

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("4").lookupPath("terms"))
                ).get();
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("1").lookupPath("arr.term"))
                ).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("2").lookupPath("arr.term"))
                ).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("3").lookupPath("arr.term"))
                ).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "2", "4");

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("not_exists").lookupIndex("lookup2").lookupType("type").lookupId("3").lookupPath("arr.term"))
                ).get();
        assertHitCount(searchResponse, 0l);
    }

    @Test
    public void testBasicFilterById() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type2", "2").setSource("field1", "value2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setFilter(idsFilter("type1").ids("1")).get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsFilter("type1", "type2").ids("1", "2"))).get();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setFilter(idsFilter().ids("1")).get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setFilter(idsFilter().ids("1", "2")).get();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsFilter().ids("1", "2"))).get();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsFilter("type1").ids("1", "2"))).get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsFilter().ids("1"))).get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsFilter(null).ids("1"))).get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsFilter("type1", "type2", "type3").ids("1", "2", "3", "4"))).get();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
    }

    @Test
    public void testBasicQueryById() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type2", "2").setSource("field1", "value2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(idsQuery("type1", "type2").ids("1", "2")).get();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(idsQuery().ids("1")).get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery().ids("1", "2")).get();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));


        searchResponse = client().prepareSearch().setQuery(idsQuery("type1").ids("1", "2")).get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery().ids("1")).get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery(null).ids("1")).get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1", "type2", "type3").ids("1", "2", "3", "4")).get();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
    }

    @Test
    public void testNumericTermsAndRanges() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1)
                .addMapping("type1",
                        "num_byte", "type=byte", "num_short", "type=short",
                        "num_integer", "type=integer", "num_long", "type=long",
                        "num_float", "type=float", "num_double", "type=double"));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("num_byte", 1, "num_short", 1, "num_integer", 1,
                "num_long", 1, "num_float", 1, "num_double", 1).get();

        client().prepareIndex("test", "type1", "2").setSource("num_byte", 2, "num_short", 2, "num_integer", 2,
                "num_long", 2, "num_float", 2, "num_double", 2).get();

        client().prepareIndex("test", "type1", "17").setSource("num_byte", 17, "num_short", 17, "num_integer", 17,
                "num_long", 17, "num_float", 17, "num_double", 17).get();
        refresh();

        SearchResponse searchResponse;
        logger.info("--> term query on 1");
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_byte", 1)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_short", 1)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_integer", 1)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_long", 1)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_float", 1)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_double", 1)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));

        logger.info("--> terms query on 1");
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_byte", new int[]{1})).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_short", new int[]{1})).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_integer", new int[]{1})).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_long", new int[]{1})).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_float", new double[]{1})).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_double", new double[]{1})).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));

        logger.info("--> term filter on 1");
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_byte", 1))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_short", 1))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_integer", 1))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_long", 1))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_float", 1))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_double", 1))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));

        logger.info("--> terms filter on 1");
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_byte", new int[]{1}))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_short", new int[]{1}))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_integer", new int[]{1}))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_long", new int[]{1}))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_float", new int[]{1}))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_double", new int[]{1}))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
    }

    @Test
    public void testNumericRangeFilter_2826() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0)
                .addMapping("type1",
                        "num_byte", "type=byte", "num_short", "type=short",
                        "num_integer", "type=integer", "num_long", "type=long",
                        "num_float", "type=float", "num_double", "type=double"));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "test1", "num_long", 1).get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "test1", "num_long", 2).get();
        client().prepareIndex("test", "type1", "3").setSource("field1", "test2", "num_long", 3).get();
        client().prepareIndex("test", "type1", "4").setSource("field1", "test2", "num_long", 4).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setFilter(
                boolFilter()
                        .should(rangeFilter("num_long", 1, 2))
                        .should(rangeFilter("num_long", 3, 4))
        ).get();
        assertHitCount(searchResponse, 4l);

        // This made 2826 fail! (only with bit based filters)
        searchResponse = client().prepareSearch("test").setFilter(
                boolFilter()
                        .should(rangeFilter("num_long", 1, 2))
                        .should(rangeFilter("num_long", 3, 4))
        ).get();
        assertHitCount(searchResponse, 4l);

        // This made #2979 fail!
        searchResponse = client().prepareSearch("test").setFilter(
                boolFilter()
                        .must(termFilter("field1", "test1"))
                        .should(rangeFilter("num_long", 1, 2))
                        .should(rangeFilter("num_long", 3, 4))
        ).get();
        assertHitCount(searchResponse, 2l);
    }

    @Test
    public void testEmptyTopLevelFilter() {
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefresh(true).get();
        SearchResponse searchResponse = client().prepareSearch().setFilter("{}").get();
        assertHitCount(searchResponse, 1l);
    }

    @Test // see #2926
    public void testMustNot() throws ElasticSearchException, IOException, ExecutionException, InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 2, SETTING_NUMBER_OF_REPLICAS, 0));
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("description", "foo other anything bar"),
                client().prepareIndex("test", "test", "2").setSource("description", "foo other anything"),
                client().prepareIndex("test", "test", "3").setSource("description", "foo other"),
                client().prepareIndex("test", "test", "4").setSource("description", "foo"));

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).get();
        assertHitCount(searchResponse, 4l);

        searchResponse = client().prepareSearch("test").setQuery(
                boolQuery()
                        .mustNot(matchQuery("description", "anything").type(Type.BOOLEAN))
        ).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).get();
        assertHitCount(searchResponse, 2l);
    }

    @Test // see #2994
    public void testSimpleSpan() throws ElasticSearchException, IOException, ExecutionException, InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0));
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("description", "foo other anything bar"),
                client().prepareIndex("test", "test", "2").setSource("description", "foo other anything"),
                client().prepareIndex("test", "test", "3").setSource("description", "foo other"),
                client().prepareIndex("test", "test", "4").setSource("description", "foo"));

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(spanOrQuery().clause(spanTermQuery("description", "bar"))).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test")
                .setQuery(spanOrQuery().clause(spanTermQuery("test.description", "bar"))).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test").setQuery(
                spanNearQuery()
                        .clause(spanTermQuery("description", "foo"))
                        .clause(spanTermQuery("test.description", "other"))
                        .slop(3)).get();
        assertHitCount(searchResponse, 3l);
    }

    @Test
    public void testSpanMultiTermQuery() throws ElasticSearchException, IOException {
        assertAcked(prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0));
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource("description", "foo other anything bar", "count", 1).get();
        client().prepareIndex("test", "test", "2").setSource("description", "foo other anything", "count", 2).get();
        client().prepareIndex("test", "test", "3").setSource("description", "foo other", "count", 3).get();
        client().prepareIndex("test", "test", "4").setSource("description", "fop", "count", 4).get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(spanOrQuery().clause(spanMultiTermQueryBuilder(fuzzyQuery("description", "fop")))).get();
        assertHitCount(response, 4);

        response = client().prepareSearch("test")
                .setQuery(spanOrQuery().clause(spanMultiTermQueryBuilder(prefixQuery("description", "fo")))).get();
        assertHitCount(response, 4);

        response = client().prepareSearch("test")
                .setQuery(spanOrQuery().clause(spanMultiTermQueryBuilder(wildcardQuery("description", "oth*")))).get();
        assertHitCount(response, 3);

        response = client().prepareSearch("test")
                .setQuery(spanOrQuery().clause(spanMultiTermQueryBuilder(rangeQuery("description").from("ffa").to("foo"))))
                .execute().actionGet();
        assertHitCount(response, 3);

        response = client().prepareSearch("test")
                .setQuery(spanOrQuery().clause(spanMultiTermQueryBuilder(regexpQuery("description", "fo{2}")))).get();
        assertHitCount(response, 3);
    }

    @Test
    public void testSimpleDFSQuery() throws ElasticSearchException, IOException {
        assertAcked(prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 5, SETTING_NUMBER_OF_REPLICAS, 0)
            .addMapping("s", jsonBuilder()
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
            .addMapping("bs", "online", "type=boolean", "ts", "type=date,ignore_malformed=false,format=dateOptionalTime"));
        ensureGreen();

        client().prepareIndex("test", "s", "1").setSource("online", false, "bs", "Y", "ts", System.currentTimeMillis() - 100).get();
        client().prepareIndex("test", "s", "2").setSource("online", true, "bs", "X", "ts", System.currentTimeMillis() - 10000000).get();
        client().prepareIndex("test", "bs", "3").setSource("online", false, "ts", System.currentTimeMillis() - 100).get();
        client().prepareIndex("test", "bs", "4").setSource("online", true, "ts", System.currentTimeMillis() - 123123).get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(
                        boolQuery()
                                .must(termQuery("online", true))
                                .must(boolQuery()
                                        .should(boolQuery()
                                                .must(rangeQuery("ts").lt(System.currentTimeMillis() - (15 * 1000)))
                                                .must(termQuery("_type", "bs"))
                                        )
                                        .should(boolQuery()
                                                .must(rangeQuery("ts").lt(System.currentTimeMillis() - (15 * 1000)))
                                                .must(termQuery("_type", "s"))
                                        )
                                )
                )
                .setVersion(true)
                .setFrom(0).setSize(100).setExplain(true).get();
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
        assertAcked(builder.addMapping("test", "text", "type=string,index_analyzer=index,search_analyzer=search"));
        ensureGreen();
        client().prepareIndex("test", "test", "1").setSource("text", "quick brown fox").get();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "quick").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "quick brown").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "fast").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        
        client().prepareIndex("test", "test", "2").setSource("text", "fast brown fox").get();
        refresh();
        searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "quick").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 2);
        searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "quick brown").operator(MatchQueryBuilder.Operator.AND)).get();
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
        assertAcked(builder.addMapping("test", "text", "type=string,index_analyzer=index,search_analyzer=search"));
        ensureGreen();
        client().prepareIndex("test", "test", "1").setSource("text", "the fox runs across the street").get();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "fox runs").operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);

        client().prepareIndex("test", "test", "2").setSource("text", "run fox run").get();
        refresh();
        searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "fox runs").operator(MatchQueryBuilder.Operator.AND)).get();
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
        assertAcked(builder.addMapping("test", "text", "type=string,index_analyzer=index,search_analyzer=search"));
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource("text", "quick brown fox").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(queryString("quick").defaultField("text").defaultOperator(QueryStringQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch("test").setQuery(queryString("quick brown").defaultField("text").defaultOperator(QueryStringQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch().setQuery(queryString("fast").defaultField("text").defaultOperator(QueryStringQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        
        client().prepareIndex("test", "test", "2").setSource("text", "fast brown fox").get();
        refresh();

        searchResponse = client().prepareSearch("test").setQuery(queryString("quick").defaultField("text").defaultOperator(QueryStringQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 2);
        searchResponse = client().prepareSearch("test").setQuery(queryString("quick brown").defaultField("text").defaultOperator(QueryStringQueryBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 2);
    }

    @Test // see https://github.com/elasticsearch/elasticsearch/issues/3898
    public void testCustomWordDelimiterQueryString() {
        assertAcked(client().admin().indices().prepareCreate("test")
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
                .addMapping("type1", "field1", "type=string,analyzer=my_analyzer", "field2", "type=string,analyzer=my_analyzer"));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "foo bar baz", "field2", "not needed").get();
        refresh();

        SearchResponse response = client()
                .prepareSearch("test")
                .setQuery(
                        queryString("foo.baz").useDisMax(false).defaultOperator(QueryStringQueryBuilder.Operator.AND)
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
                .setQuery(multiMatchQuery("value2", "field1^2", "field2").lenient(true).useDisMax(false)).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("value2", "field1^2", "field2").lenient(true).useDisMax(true)).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("value2", "field2^2").lenient(true)).get();
        assertHitCount(searchResponse, 1l);
    }

    @Test
    public void testIndicesQuery() throws Exception {
        createIndex("index1", "index2", "index3");
        ensureGreen();

        client().prepareIndex("index1", "type1").setId("1").setSource("text", "value1").get();
        client().prepareIndex("index2", "type2").setId("2").setSource("text", "value2").get();
        client().prepareIndex("index3", "type3").setId("3").setSource("text", "value3").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("index1", "index2", "index3")
                .setQuery(indicesQuery(matchQuery("text", "value1"), "index1")
                        .noMatchQuery(matchQuery("text", "value2"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "2");

        //default no match query is match_all
        searchResponse = client().prepareSearch("index1", "index2", "index3")
                .setQuery(indicesQuery(matchQuery("text", "value1"), "index1")).get();
        assertHitCount(searchResponse, 3l);
        assertSearchHits(searchResponse, "1", "2", "3");
        searchResponse = client().prepareSearch("index1", "index2", "index3")
                .setQuery(indicesQuery(matchQuery("text", "value1"), "index1")
                        .noMatchQuery("all")).get();
        assertHitCount(searchResponse, 3l);
        assertSearchHits(searchResponse, "1", "2", "3");

        searchResponse = client().prepareSearch("index1", "index2", "index3")
                .setQuery(indicesQuery(matchQuery("text", "value1"), "index1")
                        .noMatchQuery("none")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
    }

    @Test
    public void testIndicesFilter() throws Exception {
        createIndex("index1", "index2", "index3");
        ensureGreen();

        client().prepareIndex("index1", "type1").setId("1").setSource("text", "value1").get();
        client().prepareIndex("index2", "type2").setId("2").setSource("text", "value2").get();
        client().prepareIndex("index3", "type3").setId("3").setSource("text", "value3").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("index1", "index2", "index3")
                .setFilter(indicesFilter(termFilter("text", "value1"), "index1")
                        .noMatchFilter(termFilter("text", "value2"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "2");

        //default no match filter is "all"
        searchResponse = client().prepareSearch("index1", "index2", "index3")
                .setFilter(indicesFilter(termFilter("text", "value1"), "index1")).get();
        assertHitCount(searchResponse, 3l);
        assertSearchHits(searchResponse, "1", "2", "3");

        searchResponse = client().prepareSearch("index1", "index2", "index3")
                .setFilter(indicesFilter(termFilter("text", "value1"), "index1")
                        .noMatchFilter("all")).get();
        assertHitCount(searchResponse, 3l);
        assertSearchHits(searchResponse, "1", "2", "3");

        searchResponse = client().prepareSearch("index1", "index2", "index3")
                .setFilter(indicesFilter(termFilter("text", "value1"), "index1")
                        .noMatchFilter("none")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("1"));
    }

    @Test // https://github.com/elasticsearch/elasticsearch/issues/2416
    public void testIndicesQuerySkipParsing() throws Exception {
        createIndex("simple");
        client().admin().indices().prepareCreate("related")
                .addMapping("child", jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent")
                        .endObject().endObject().endObject()).get();
        ensureGreen();

        client().prepareIndex("simple", "lone").setId("1").setSource("text", "value1").get();
        client().prepareIndex("related", "parent").setId("2").setSource("text", "parent").get();
        client().prepareIndex("related", "child").setId("3").setParent("2").setSource("text", "value2").get();
        refresh();

        //has_child fails if executed on "simple" index
        try {
            client().prepareSearch("simple")
                    .setQuery(hasChildQuery("child", matchQuery("text", "value"))).get();
            fail("Should have failed as has_child query can only be executed against parent-child types");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.shardFailures().length, greaterThan(0));
            for (ShardSearchFailure shardSearchFailure : e.shardFailures()) {
                assertThat(shardSearchFailure.reason(), containsString("No mapping for for type [child]"));
            }
        }

        //has_child doesn't get parsed for "simple" index
        SearchResponse searchResponse = client().prepareSearch("related", "simple")
                .setQuery(indicesQuery(hasChildQuery("child", matchQuery("text", "value2")), "related")
                        .noMatchQuery(matchQuery("text", "value1"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "2");
    }

    @Test // https://github.com/elasticsearch/elasticsearch/issues/2416
    public void testIndicesFilterSkipParsing() throws Exception {
        createIndex("simple");
        client().admin().indices().prepareCreate("related")
                .addMapping("child", jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent")
                        .endObject().endObject().endObject()).get();
        ensureGreen();

        client().prepareIndex("simple", "lone").setId("1").setSource("text", "value1").get();
        client().prepareIndex("related", "parent").setId("2").setSource("text", "parent").get();
        client().prepareIndex("related", "child").setId("3").setParent("2").setSource("text", "value2").get();
        refresh();

        //has_child fails if executed on "simple" index
        try {
            client().prepareSearch("simple")
                    .setFilter(hasChildFilter("child", termFilter("text", "value1"))).get();
            fail("Should have failed as has_child query can only be executed against parent-child types");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.shardFailures().length, greaterThan(0));
            for (ShardSearchFailure shardSearchFailure : e.shardFailures()) {
                assertThat(shardSearchFailure.reason(), containsString("No mapping for for type [child]"));
            }
        }

        SearchResponse searchResponse = client().prepareSearch("related", "simple")
                .setFilter(indicesFilter(hasChildFilter("child", termFilter("text", "value2")), "related")
                        .noMatchFilter(termFilter("text", "value1"))).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "1", "2");
    }

    @Test
    public void testIndicesQueryMissingIndices() throws IOException {
        createIndex("index1");
        createIndex("index2");
        ensureGreen();

        client().prepareIndex("index1", "type1", "1").setSource("field", "match").get();
        client().prepareIndex("index1", "type1", "2").setSource("field", "no_match").get();
        client().prepareIndex("index2", "type1", "10").setSource("field", "match").get();
        client().prepareIndex("index2", "type1", "20").setSource("field", "no_match").get();
        client().prepareIndex("index3", "type1", "100").setSource("field", "match").get();
        client().prepareIndex("index3", "type1", "200").setSource("field", "no_match").get();
        refresh();

        //all indices are missing
        SearchResponse searchResponse = client().prepareSearch().setQuery(
                indicesQuery(termQuery("field", "missing"), "test1", "test2", "test3")
                        .noMatchQuery(termQuery("field", "match"))).get();

        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            if ("index1".equals(hit.index())) {
                assertThat(hit, hasId("1"));
            } else if ("index2".equals(hit.index())) {
                assertThat(hit, hasId("10"));
            } else if ("index3".equals(hit.index())) {
                assertThat(hit, hasId("100"));
            } else {
                fail("Returned documents should belong to either index1, index2 or index3");
            }
        }

        //only one index specified, which is missing
        searchResponse = client().prepareSearch().setQuery(
                indicesQuery(termQuery("field", "missing"), "test1")
                        .noMatchQuery(termQuery("field", "match"))).get();

        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            if ("index1".equals(hit.index())) {
                assertThat(hit, hasId("1"));
            } else if ("index2".equals(hit.index())) {
                assertThat(hit, hasId("10"));
            } else if ("index3".equals(hit.index())) {
                assertThat(hit, hasId("100"));
            } else {
                fail("Returned documents should belong to either index1, index2 or index3");
            }
        }

        //more than one index specified, one of them is missing
        searchResponse = client().prepareSearch().setQuery(
                indicesQuery(termQuery("field", "missing"), "index1", "test1")
                        .noMatchQuery(termQuery("field", "match"))).get();

        assertHitCount(searchResponse, 2l);

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            if ("index2".equals(hit.index())) {
                assertThat(hit, hasId("10"));
            } else if ("index3".equals(hit.index())) {
                assertThat(hit, hasId("100"));
            } else {
                fail("Returned documents should belong to either index2 or index3");
            }
        }
    }

    @Test
    public void testIndicesFilterMissingIndices() throws IOException {
        createIndex("index1");
        createIndex("index2");
        ensureGreen();

        client().prepareIndex("index1", "type1", "1").setSource("field", "match").get();
        client().prepareIndex("index1", "type1", "2").setSource("field", "no_match").get();
        client().prepareIndex("index2", "type1", "10").setSource("field", "match").get();
        client().prepareIndex("index2", "type1", "20").setSource("field", "no_match").get();
        client().prepareIndex("index3", "type1", "100").setSource("field", "match").get();
        client().prepareIndex("index3", "type1", "200").setSource("field", "no_match").get();
        refresh();

        //all indices are missing
        SearchResponse searchResponse = client().prepareSearch().setQuery(
                filteredQuery(matchAllQuery(),
                        indicesFilter(termFilter("field", "missing"), "test1", "test2", "test3")
                                .noMatchFilter(termFilter("field", "match")))).get();

        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            if ("index1".equals(hit.index())) {
                assertThat(hit, hasId("1"));
            } else if ("index2".equals(hit.index())) {
                assertThat(hit, hasId("10"));
            } else if ("index3".equals(hit.index())) {
                assertThat(hit, hasId("100"));
            } else {
                fail("Returned documents should belong to either index1, index2 or index3");
            }
        }

        //only one index specified, which is missing
        searchResponse = client().prepareSearch().setQuery(
                filteredQuery(matchAllQuery(),
                        indicesFilter(termFilter("field", "missing"), "test1")
                                .noMatchFilter(termFilter("field", "match")))).get();

        assertHitCount(searchResponse, 3l);

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            if ("index1".equals(hit.index())) {
                assertThat(hit, hasId("1"));
            } else if ("index2".equals(hit.index())) {
                assertThat(hit, hasId("10"));
            } else if ("index3".equals(hit.index())) {
                assertThat(hit, hasId("100"));
            } else {
                fail("Returned documents should belong to either index1, index2 or index3");
            }
        }

        //more than one index specified, one of them is missing
        searchResponse = client().prepareSearch().setQuery(
                filteredQuery(matchAllQuery(),
                        indicesFilter(termFilter("field", "missing"), "index1", "test1")
                                .noMatchFilter(termFilter("field", "match")))).get();

        assertHitCount(searchResponse, 2l);

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            if ("index2".equals(hit.index())) {
                assertThat(hit, hasId("10"));
            } else if ("index3".equals(hit.index())) {
                assertThat(hit, hasId("100"));
            } else {
                fail("Returned documents should belong to either index2 or index3");
            }
        }
    }

    @Test
    public void testMinScore() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource("score", 1.5).get();
        client().prepareIndex("test", "test", "2").setSource("score", 1).get();
        client().prepareIndex("test", "test", "3").setSource("score", 2).get();
        client().prepareIndex("test", "test", "4").setSource("score", 0.5).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(
                functionScoreQuery(scriptFunction("_doc['score'].value"))).setMinScore(1.5f).get();
        assertHitCount(searchResponse, 2);
        assertFirstHit(searchResponse, hasId("3"));
        assertSecondHit(searchResponse, hasId("1"));
    }

    @Test
    public void testQueryStringWithSlopAndFields() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "customer", "1").setSource("desc", "one two three").get();
        client().prepareIndex("test", "product", "2").setSource("desc", "one two three").get();
        refresh();
        {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryString("\"one two\"").defaultField("desc")).get();
            assertHitCount(searchResponse, 2);
        }
        {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryString("\"one two\"").field("product.desc")).get();
            assertHitCount(searchResponse, 1);
        }
        {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryString("\"one three\"~5").field("product.desc")).get();
            assertHitCount(searchResponse, 1);
        }
        {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryString("\"one two\"").defaultField("customer.desc")).get();
            assertHitCount(searchResponse, 1);
        }
        {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryString("\"one two\"").defaultField("customer.desc")).get();
            assertHitCount(searchResponse, 1);
        }
    }

    private static FilterBuilder rangeFilter(String field, Object from, Object to) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return FilterBuilders.rangeFilter(field).from(from).to(to);
            } else {
                return FilterBuilders.rangeFilter(field).from(from).to(to).setExecution("fielddata");
            }
        } else {
            return FilterBuilders.numericRangeFilter(field).from(from).to(to);
        }
    }

    @Test
    public void testSimpleQueryString() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));
        client().prepareIndex("test", "type1", "1").setSource("body", "foo").get();
        client().prepareIndex("test", "type1", "2").setSource("body", "bar").get();
        client().prepareIndex("test", "type1", "3").setSource("body", "foo bar").get();
        client().prepareIndex("test", "type1", "4").setSource("body", "quux baz eggplant").get();
        client().prepareIndex("test", "type1", "5").setSource("body", "quux baz spaghetti").get();
        client().prepareIndex("test", "type1", "6").setSource("otherbody", "spaghetti").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryString("foo bar")).get();
        assertHitCount(searchResponse, 3l);
        assertSearchHits(searchResponse, "1", "2", "3");

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryString("foo bar").defaultOperator(SimpleQueryStringBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(simpleQueryString("\"quux baz\" +(eggplant | spaghetti)")).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "4", "5");

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryString("eggplants").analyzer("snowball")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("4"));

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryString("spaghetti").field("body", 10.0f).field("otherbody", 2.0f)).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasId("5"));
        assertSearchHits(searchResponse, "5", "6");
    }
}
