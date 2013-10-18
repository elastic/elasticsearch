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

package org.elasticsearch.count.query;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.CommonTermsQueryBuilder.Operator;
import org.elasticsearch.index.query.MatchQueryBuilder.Type;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleQueryTests extends AbstractIntegrationTest {

    @Test
    public void passQueryAsStringTest() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings("index.number_of_shards", 1).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefresh(true).execute().actionGet();

        CountResponse countResponse = client().prepareCount().setQuery(new BytesArray("{ \"term\" : { \"field1\" : \"value1_1\" }}").array()).execute().actionGet();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testIndexOptions() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,index_options=docs")
                .setSettings("index.number_of_shards", 1).get();

        client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox").setRefresh(true).execute().actionGet();

        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.matchQuery("field2", "quick brown").type(Type.PHRASE).slop(0)).get();
        assertHitCount(countResponse, 1l);
        try {
            client().prepareCount().setQuery(QueryBuilders.matchQuery("field1", "quick brown").type(Type.PHRASE).slop(0)).get();
        } catch (SearchPhaseExecutionException e) {
            assertTrue("wrong exception message " + e.getMessage(), e.getMessage().endsWith("IllegalStateException[field \"field1\" was indexed without position data; cannot run PhraseQuery (term=quick)]; }"));
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

        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.commonTerms("field1", "the quick brown").cutoffFrequency(3).lowFreqOperator(Operator.OR)).execute().actionGet();
        assertHitCount(countResponse, 3l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTerms("field1", "the quick brown").cutoffFrequency(3).lowFreqOperator(Operator.AND)).execute().actionGet();
        assertHitCount(countResponse, 2l);

        // Default
        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTerms("field1", "the quick brown").cutoffFrequency(3)).execute().actionGet();
        assertHitCount(countResponse, 3l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTerms("field1", "the huge fox").lowFreqMinimumShouldMatch("2")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTerms("field1", "the lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("3")).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTerms("field1", "the lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("4")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(new BytesArray("{ \"common\" : { \"field1\" : { \"query\" : \"the lazy fox brown\", \"cutoff_frequency\" : 1, \"minimum_should_match\" : { \"high_freq\" : 4 } } } }").array()).execute().actionGet();
        assertHitCount(countResponse, 1l);

        // Default
        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTerms("field1", "the lazy fox brown").cutoffFrequency(1)).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTerms("field1", "the quick brown").cutoffFrequency(3).analyzer("standard")).execute().actionGet();
        assertHitCount(countResponse, 3l);
        // standard drops "the" since its a stopword

        // try the same with match query
        countResponse = client().prepareCount().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND)).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.OR)).execute().actionGet();
        assertHitCount(countResponse, 3l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND).analyzer("standard")).execute().actionGet();
        assertHitCount(countResponse, 3l);
        // standard drops "the" since its a stopword

        // try the same with multi match query
        countResponse = client().prepareCount().setQuery(QueryBuilders.multiMatchQuery("the quick brown", "field1", "field2").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND)).execute().actionGet();
        assertHitCount(countResponse, 3l);
    }

    @Test
    public void testOmitTermFreqsAndPositions() throws Exception {
        // backwards compat test!
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,omit_term_freq_and_positions=true")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).get();

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox"));


        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.matchQuery("field2", "quick brown").type(Type.PHRASE).slop(0)).execute().actionGet();
        assertHitCount(countResponse, 1l);
        try {
            client().prepareCount().setQuery(QueryBuilders.matchQuery("field1", "quick brown").type(Type.PHRASE).slop(0)).execute().actionGet();
        } catch (SearchPhaseExecutionException e) {
            assertTrue(e.getMessage().endsWith("IllegalStateException[field \"field1\" was indexed without position data; cannot run PhraseQuery (term=quick)]; }"));
        }
    }

    @Test
    public void queryStringAnalyzedWildcard() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        CountResponse countResponse = client().prepareCount().setQuery(queryString("value*").analyzeWildcard(true)).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryString("*ue*").analyzeWildcard(true)).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryString("*ue_1").analyzeWildcard(true)).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryString("val*e_1").analyzeWildcard(true)).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryString("v?l*e?1").analyzeWildcard(true)).execute().actionGet();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testLowercaseExpandedTerms() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        CountResponse countResponse = client().prepareCount().setQuery(queryString("VALUE_3~1").lowercaseExpandedTerms(true)).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount().setQuery(queryString("VALUE_3~1").lowercaseExpandedTerms(false)).execute().actionGet();
        assertHitCount(countResponse, 0l);
        countResponse = client().prepareCount().setQuery(queryString("ValUE_*").lowercaseExpandedTerms(true)).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount().setQuery(queryString("vAl*E_1")).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount().setQuery(queryString("[VALUE_1 TO VALUE_3]")).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount().setQuery(queryString("[VALUE_1 TO VALUE_3]").lowercaseExpandedTerms(false)).execute().actionGet();
        assertHitCount(countResponse, 0l);
    }

    @Test
    public void testDateRangeInQueryString() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        String aMonthAgo = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).minusMonths(1));
        String aMonthFromNow = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).plusMonths(1));

        client().prepareIndex("test", "type", "1").setSource("past", aMonthAgo, "future", aMonthFromNow).execute().actionGet();

        refresh();

        CountResponse countResponse = client().prepareCount().setQuery(queryString("past:[now-2M/d TO now/d]")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryString("future:[now/d TO now+2M/d]").lowercaseExpandedTerms(false)).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryString("future:[now/D TO now+2M/d]").lowercaseExpandedTerms(false)).execute().actionGet();
        //D is an unsupported unit in date math
        assertThat(countResponse.getSuccessfulShards(), equalTo(0));
        assertThat(countResponse.getFailedShards(), equalTo(1));
        assertThat(countResponse.getShardFailures().length, equalTo(1));
        assertThat(countResponse.getShardFailures()[0].reason(), allOf(containsString("Failed to parse"), containsString("unit [D] not supported for date math")));
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

        assertHitCount(client().prepareCount().setQuery(filteredQuery(matchAllQuery(), typeFilter("type1"))).get(), 2l);
        assertHitCount(client().prepareCount().setQuery(filteredQuery(matchAllQuery(), typeFilter("type2"))).get(), 3l);

        assertHitCount(client().prepareCount().setTypes("type1").setQuery(matchAllQuery()).execute().actionGet(), 2l);
        assertHitCount(client().prepareCount().setTypes("type2").setQuery(matchAllQuery()).execute().actionGet(), 3l);

        assertHitCount(client().prepareCount().setTypes("type1", "type2").setQuery(matchAllQuery()).execute().actionGet(), 5l);
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

        CountResponse countResponse = client().prepareCount().setQuery(constantScoreQuery(idsFilter("type1").ids("1", "3"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        // no type
        countResponse = client().prepareCount().setQuery(constantScoreQuery(idsFilter().ids("1", "3"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(idsQuery("type1").ids("1", "3")).execute().actionGet();
        assertHitCount(countResponse, 2l);

        // no type
        countResponse = client().prepareCount().setQuery(idsQuery().ids("1", "3")).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(idsQuery("type1").ids("7", "10")).execute().actionGet();
        assertHitCount(countResponse, 0l);

        // repeat..., with terms
        countResponse = client().prepareCount().setTypes("type1").setQuery(constantScoreQuery(termsFilter("_id", "1", "3"))).execute().actionGet();
        assertHitCount(countResponse, 2l);
    }

    @Test
    public void testLimitFilter() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value1_2").execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource("field2", "value2_3").execute().actionGet();
        client().prepareIndex("test", "type1", "4").setSource("field3", "value3_4").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        CountResponse countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), limitFilter(2))).execute().actionGet();
        assertHitCount(countResponse, 2l);
    }

    @Test
    public void filterExistsMissingTests() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x1", "x_1").field("field1", "value1_1").field("field2", "value2_1").endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x2", "x_2").field("field1", "value1_2").endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y1", "y_1").field("field2", "value2_3").endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y2", "y_2").field("field3", "value3_4").endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        CountResponse countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), existsFilter("field1"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(constantScoreQuery(existsFilter("field1"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(queryString("_exists_:field1")).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), existsFilter("field2"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), existsFilter("field3"))).execute().actionGet();
        assertHitCount(countResponse, 1l);

        // wildcard check
        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), existsFilter("x*"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        // object check
        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), existsFilter("obj1"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(constantScoreQuery(missingFilter("field1"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(queryString("_missing_:field1")).execute().actionGet();
        assertHitCount(countResponse, 2l);

        // wildcard check
        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), missingFilter("x*"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        // object check
        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), missingFilter("obj1"))).execute().actionGet();
        assertHitCount(countResponse, 2l);
    }

    @Test
    public void passQueryAsJSONStringTest() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefresh(true).execute().actionGet();

        WrapperQueryBuilder wrapper = new WrapperQueryBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }");
        CountResponse countResponse = client().prepareCount().setQuery(wrapper).execute().actionGet();
        assertHitCount(countResponse, 1l);

        BoolQueryBuilder bool = new BoolQueryBuilder();
        bool.must(wrapper);
        bool.must(new TermQueryBuilder("field2", "value2_1"));

        countResponse = client().prepareCount().setQuery(wrapper).execute().actionGet();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testFiltersWithCustomCacheKey() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
        refresh();
        CountResponse countResponse = client().prepareCount("test").setQuery(constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1"))).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1"))).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(constantScoreQuery(termsFilter("field1", "value1"))).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(constantScoreQuery(termsFilter("field1", "value1"))).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testMatchQueryNumeric() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("long", 1l, "double", 1.0d).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("long", 2l, "double", 2.0d).execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource("long", 3l, "double", 3.0d).execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        CountResponse countResponse = client().prepareCount().setQuery(matchQuery("long", "1")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(matchQuery("double", "2")).execute().actionGet();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testMultiMatchQuery() throws Exception {

        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value4", "field3", "value3").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2", "field2", "value5", "field3", "value2").execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource("field1", "value3", "field2", "value6", "field3", "value1").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        MultiMatchQueryBuilder builder = QueryBuilders.multiMatchQuery("value1 value2 value4", "field1", "field2");
        CountResponse countResponse = client().prepareCount()
                .setQuery(builder)
                .execute().actionGet();

        assertHitCount(countResponse, 2l);

        client().admin().indices().prepareRefresh("test").execute().actionGet();
        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field2")
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        countResponse = client().prepareCount()
                .setQuery(builder)
                .execute().actionGet();
        assertHitCount(countResponse, 1l);

        client().admin().indices().prepareRefresh("test").execute().actionGet();
        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field3^1.5")
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        countResponse = client().prepareCount()
                .setQuery(builder)
                .execute().actionGet();
        assertHitCount(countResponse, 2l);

        client().admin().indices().prepareRefresh("test").execute().actionGet();
        builder = QueryBuilders.multiMatchQuery("value1").field("field1").field("field3", 1.5f)
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        countResponse = client().prepareCount()
                .setQuery(builder)
                .execute().actionGet();
        assertHitCount(countResponse, 2l);

        // Test lenient
        client().prepareIndex("test", "type1", "3").setSource("field1", "value7", "field2", "value8", "field4", 5).execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field2", "field4");
        builder.lenient(true);
        countResponse = client().prepareCount().setQuery(builder).execute().actionGet();
        assertHitCount(countResponse, 1l);
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
        CountResponse countResponse = client().prepareCount()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 0l);

        boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        countResponse = client().prepareCount()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 1l);

        boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        countResponse = client().prepareCount()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 2l);
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
        CountResponse countResponse = client().prepareCount()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 0l);

        boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL))
                .must(multiMatchQuery("value4", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        countResponse = client().prepareCount()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 1l);

        boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        countResponse = client().prepareCount()
                .setQuery(boolQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 2l);
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
        CountResponse countResponse = client().prepareCount()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 1l);

        multiMatchQuery.minimumShouldMatch("30%");
        countResponse = client().prepareCount()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 2l);

        multiMatchQuery.useDisMax(false);
        multiMatchQuery.minimumShouldMatch("70%");
        countResponse = client().prepareCount()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 1l);

        multiMatchQuery.minimumShouldMatch("30%");
        countResponse = client().prepareCount()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 2l);

        multiMatchQuery = multiMatchQuery("value1 value2 bar", "field1");
        multiMatchQuery.minimumShouldMatch("100%");
        countResponse = client().prepareCount()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 0l);

        multiMatchQuery.minimumShouldMatch("70%");
        countResponse = client().prepareCount()
                .setQuery(multiMatchQuery)
                .execute().actionGet();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testFuzzyQueryString() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "type1", "1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("str", "shay", "date", "2012-02-05", "num", 20).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        CountResponse countResponse = client().prepareCount()
                .setQuery(queryString("str:kimcy~1"))
                .execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount()
                .setQuery(queryString("num:11~1"))
                .execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount()
                .setQuery(queryString("date:2012-02-02~1d"))
                .execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testSpecialRangeSyntaxInQueryString() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "type1", "1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("str", "shay", "date", "2012-02-05", "num", 20).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        CountResponse countResponse = client().prepareCount()
                .setQuery(queryString("num:>19"))
                .execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount()
                .setQuery(queryString("num:>20"))
                .execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 0l);

        countResponse = client().prepareCount()
                .setQuery(queryString("num:>=20"))
                .execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount()
                .setQuery(queryString("num:>11"))
                .execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount()
                .setQuery(queryString("num:<20"))
                .execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount()
                .setQuery(queryString("num:<=20"))
                .execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount()
                .setQuery(queryString("+num:>11 +num:<20"))
                .execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testEmptyTermsFilter() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", 
                jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("terms").field("type", "string").endObject()
                    .endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("test", "type", "1").setSource("term", "1").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("term", "2").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("term", "3").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("term", "4").execute().actionGet();
        refresh();
        CountResponse countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("term", new String[0]))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 0l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), idsFilter())
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 0l);
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

        CountResponse countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 2l);

        // same as above, just on the _id...
        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("_id").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 2l);

        // another search with same parameters...
        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("2").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("3").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("4").lookupPath("terms"))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 0l);


        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("1").lookupPath("arr.term"))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("2").lookupPath("arr.term"))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("3").lookupPath("arr.term"))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("not_exists").lookupIndex("lookup2").lookupType("type").lookupId("3").lookupPath("arr.term"))
                ).execute().actionGet();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 0l);
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

        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1", "type2").ids("1", "2"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter().ids("1", "2"))).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1").ids("1", "2"))).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter().ids("1"))).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter(null).ids("1"))).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1", "type2", "type3").ids("1", "2", "3", "4"))).execute().actionGet();
        assertHitCount(countResponse, 2l);
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

        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery("type1", "type2").ids("1", "2")).execute().actionGet();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery().ids("1")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery().ids("1", "2")).execute().actionGet();
        assertHitCount(countResponse, 2l);


        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery("type1").ids("1", "2")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery().ids("1")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery(null).ids("1")).execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery("type1", "type2", "type3").ids("1", "2", "3", "4")).execute().actionGet();
        assertHitCount(countResponse, 2l);
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

        CountResponse countResponse;

        logger.info("--> term query on 1");
        countResponse = client().prepareCount("test").setQuery(termQuery("num_byte", 1)).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termQuery("num_short", 1)).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termQuery("num_integer", 1)).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termQuery("num_long", 1)).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termQuery("num_float", 1)).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termQuery("num_double", 1)).execute().actionGet();
        assertHitCount(countResponse, 1l);

        logger.info("--> terms query on 1");
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_byte", new int[]{1})).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_short", new int[]{1})).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_integer", new int[]{1})).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_long", new int[]{1})).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_float", new double[]{1})).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_double", new double[]{1})).execute().actionGet();
        assertHitCount(countResponse, 1l);

        logger.info("--> term filter on 1");
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_byte", 1))).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_short", 1))).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_integer", 1))).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_long", 1))).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_float", 1))).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_double", 1))).execute().actionGet();
        assertHitCount(countResponse, 1l);

        logger.info("--> terms filter on 1");
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_byte", new int[]{1}))).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_short", new int[]{1}))).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_integer", new int[]{1}))).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_long", new int[]{1}))).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_float", new int[]{1}))).execute().actionGet();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_double", new int[]{1}))).execute().actionGet();
        assertHitCount(countResponse, 1l);
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
        CountResponse response = client().prepareCount("test").setQuery(
                filteredQuery(matchAllQuery(), FilterBuilders.boolFilter()
                        .should(FilterBuilders.rangeFilter("num_long").from(1).to(2))
                        .should(FilterBuilders.rangeFilter("num_long").from(3).to(4)))
        ).execute().actionGet();
        assertHitCount(response, 4l);

        // This made 2826 fail! (only with bit based filters)
        response = client().prepareCount("test").setQuery(
                filteredQuery(matchAllQuery(), FilterBuilders.boolFilter()
                        .should(FilterBuilders.numericRangeFilter("num_long").from(1).to(2))
                        .should(FilterBuilders.numericRangeFilter("num_long").from(3).to(4)))
        ).execute().actionGet();

        assertHitCount(response, 4l);

        // This made #2979 fail!
        response = client().prepareCount("test").setQuery(
                filteredQuery(matchAllQuery(), FilterBuilders.boolFilter()
                        .must(FilterBuilders.termFilter("field1", "test1"))
                        .should(FilterBuilders.rangeFilter("num_long").from(1).to(2))
                        .should(FilterBuilders.rangeFilter("num_long").from(3).to(4)))
        ).execute().actionGet();

        assertHitCount(response, 2l);
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

        CountResponse response = client().prepareCount("test")
                .setQuery(QueryBuilders.spanOrQuery().clause(QueryBuilders.spanTermQuery("description", "bar")))
                .execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 1l);
        response = client().prepareCount("test")
                .setQuery(QueryBuilders.spanOrQuery().clause(QueryBuilders.spanTermQuery("test.description", "bar")))
                .execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 1l);

        response = client().prepareCount("test").setQuery(
                QueryBuilders.spanNearQuery()
                        .clause(QueryBuilders.spanTermQuery("description", "foo"))
                        .clause(QueryBuilders.spanTermQuery("test.description", "other"))
                        .slop(3)).execute().actionGet();
        assertNoFailures(response);
        assertHitCount(response, 3l);
    }
}
