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

package org.elasticsearch.count.query;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.CommonTermsQueryBuilder.Operator;
import org.elasticsearch.index.query.MatchQueryBuilder.Type;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

public class CountQueryTests extends ElasticsearchIntegrationTest {

    @Test
    public void passQueryAsStringTest() throws Exception {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefresh(true).get();

        CountResponse countResponse = client().prepareCount().setSource(new BytesArray("{ \"query\" : { \"term\" : { \"field1\" : \"value1_1\" }}}").array()).get();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testIndexOptions() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "field1", "type=string,index_options=docs"));

        client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox").get();
        refresh();

        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.matchQuery("field2", "quick brown").type(Type.PHRASE).slop(0)).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.matchQuery("field1", "quick brown").type(Type.PHRASE).slop(0)).get();
        assertHitCount(countResponse, 0l);
        assertThat(countResponse.getFailedShards(), anyOf(equalTo(1), equalTo(2)));
        assertThat(countResponse.getFailedShards(), equalTo(countResponse.getShardFailures().length));
        for (ShardOperationFailedException shardFailure : countResponse.getShardFailures()) {
            assertThat(shardFailure.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
            assertThat(shardFailure.reason(), containsString("[field \"field1\" was indexed without position data; cannot run PhraseQuery"));
        }
    }

    @Test
    public void testCommonTermsQuery() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "field1", "type=string,analyzer=whitespace")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        indexRandom(true,
                client().prepareIndex("test", "type1", "3").setSource("field1", "quick lazy huge brown pidgin", "field2", "the quick lazy huge brown fox jumps over the tree"),
                client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "the quick lazy huge brown fox jumps over the tree") );

        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.commonTermsQuery("field1", "the quick brown").cutoffFrequency(3).lowFreqOperator(Operator.OR)).get();
        assertHitCount(countResponse, 3l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTermsQuery("field1", "the quick brown").cutoffFrequency(3).lowFreqOperator(Operator.AND)).get();
        assertHitCount(countResponse, 2l);

        // Default
        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTermsQuery("field1", "the quick brown").cutoffFrequency(3)).get();
        assertHitCount(countResponse, 3l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTermsQuery("field1", "the huge fox").lowFreqMinimumShouldMatch("2")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTermsQuery("field1", "the lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("3")).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTermsQuery("field1", "the lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("4")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setSource(new BytesArray("{ \"query\" : { \"common\" : { \"field1\" : { \"query\" : \"the lazy fox brown\", \"cutoff_frequency\" : 1, \"minimum_should_match\" : { \"high_freq\" : 4 } } } } }").array()).get();
        assertHitCount(countResponse, 1l);

        // Default
        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTermsQuery("field1", "the lazy fox brown").cutoffFrequency(1)).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.commonTermsQuery("field1", "the quick brown").cutoffFrequency(3).analyzer("standard")).get();
        assertHitCount(countResponse, 3l);
        // standard drops "the" since its a stopword

        // try the same with match query
        countResponse = client().prepareCount().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.OR)).get();
        assertHitCount(countResponse, 3l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND).analyzer("stop")).get();
        assertHitCount(countResponse, 3l);
        // standard drops "the" since its a stopword

        // try the same with multi match query
        countResponse = client().prepareCount().setQuery(QueryBuilders.multiMatchQuery("the quick brown", "field1", "field2").cutoffFrequency(3).operator(MatchQueryBuilder.Operator.AND)).get();
        assertHitCount(countResponse, 3l);
    }

    @Test
    public void queryStringAnalyzedWildcard() throws Exception {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").get();
        refresh();

        CountResponse countResponse = client().prepareCount().setQuery(queryStringQuery("value*").analyzeWildcard(true)).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("*ue*").analyzeWildcard(true)).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("*ue_1").analyzeWildcard(true)).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("val*e_1").analyzeWildcard(true)).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("v?l*e?1").analyzeWildcard(true)).get();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testLowercaseExpandedTerms() {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").get();
        refresh();

        CountResponse countResponse = client().prepareCount().setQuery(queryStringQuery("VALUE_3~1").lowercaseExpandedTerms(true)).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount().setQuery(queryStringQuery("VALUE_3~1").lowercaseExpandedTerms(false)).get();
        assertHitCount(countResponse, 0l);
        countResponse = client().prepareCount().setQuery(queryStringQuery("ValUE_*").lowercaseExpandedTerms(true)).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount().setQuery(queryStringQuery("vAl*E_1")).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount().setQuery(queryStringQuery("[VALUE_1 TO VALUE_3]")).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount().setQuery(queryStringQuery("[VALUE_1 TO VALUE_3]").lowercaseExpandedTerms(false)).get();
        assertHitCount(countResponse, 0l);
    }

    @Test //https://github.com/elasticsearch/elasticsearch/issues/3540
    public void testDateRangeInQueryString() {
        //the mapping needs to be provided upfront otherwise we are not sure how many failures we get back
        //as with dynamic mappings some shards might be lacking behind and parse a different query
        assertAcked(prepareCreate("test").addMapping(
                "type", "past", "type=date", "future", "type=date"
        ));
        ensureGreen();

        NumShards test = getNumShards("test");

        String aMonthAgo = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).minusMonths(1));
        String aMonthFromNow = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).plusMonths(1));

        client().prepareIndex("test", "type", "1").setSource("past", aMonthAgo, "future", aMonthFromNow).get();
        refresh();

        CountResponse countResponse = client().prepareCount().setQuery(queryStringQuery("past:[now-2M/d TO now/d]")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("future:[now/d TO now+2M/d]").lowercaseExpandedTerms(false)).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(queryStringQuery("future:[now/D TO now+2M/d]").lowercaseExpandedTerms(false)).get();
        //D is an unsupported unit in date math
        assertThat(countResponse.getSuccessfulShards(), equalTo(0));
        assertThat(countResponse.getFailedShards(), equalTo(test.numPrimaries));
        assertThat(countResponse.getShardFailures().length, equalTo(test.numPrimaries));
        for (ShardOperationFailedException shardFailure : countResponse.getShardFailures()) {
            assertThat(shardFailure.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(shardFailure.reason(), allOf(containsString("Failed to parse"), containsString("unit [D] not supported for date math")));
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
        Settings indexSettings = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        assertAcked(prepareCreate("test").setSettings(indexSettings)
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

        assertHitCount(client().prepareCount().setQuery(filteredQuery(matchAllQuery(), typeFilter("type1"))).get(), 2l);
        assertHitCount(client().prepareCount().setQuery(filteredQuery(matchAllQuery(), typeFilter("type2"))).get(), 3l);

        assertHitCount(client().prepareCount().setTypes("type1").setQuery(matchAllQuery()).get(), 2l);
        assertHitCount(client().prepareCount().setTypes("type2").setQuery(matchAllQuery()).get(), 3l);

        assertHitCount(client().prepareCount().setTypes("type1", "type2").setQuery(matchAllQuery()).get(), 5l);
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
        Settings indexSettings = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        assertAcked(prepareCreate("test").setSettings(indexSettings)
            .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                .startObject("_id").field("index", index).endObject()
                .endObject().endObject()));

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value2"),
                client().prepareIndex("test", "type1", "3").setSource("field1", "value3"));

        CountResponse countResponse = client().prepareCount().setQuery(constantScoreQuery(idsFilter("type1").ids("1", "3"))).get();
        assertHitCount(countResponse, 2l);

        // no type
        countResponse = client().prepareCount().setQuery(constantScoreQuery(idsFilter().ids("1", "3"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(idsQuery("type1").ids("1", "3")).get();
        assertHitCount(countResponse, 2l);

        // no type
        countResponse = client().prepareCount().setQuery(idsQuery().ids("1", "3")).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(idsQuery("type1").ids("7", "10")).get();
        assertHitCount(countResponse, 0l);

        // repeat..., with terms
        countResponse = client().prepareCount().setTypes("type1").setQuery(constantScoreQuery(termsFilter("_id", "1", "3"))).get();
        assertHitCount(countResponse, 2l);
    }

    @Test
    public void testLimitFilter() throws Exception {
        assertAcked(prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1));

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value1_2"),
                client().prepareIndex("test", "type1", "3").setSource("field2", "value2_3"),
                client().prepareIndex("test", "type1", "4").setSource("field3", "value3_4"));

        CountResponse countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), limitFilter(2))).get();
        assertHitCount(countResponse, 4l); // limit is a no-op
    }

    @Test
    public void filterExistsMissingTests() throws Exception {
        createIndex("test");

        indexRandom(true,
                client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x1", "x_1").field("field1", "value1_1").field("field2", "value2_1").endObject()),
                client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x2", "x_2").field("field1", "value1_2").endObject()),
                client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y1", "y_1").field("field2", "value2_3").endObject()),
                client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y2", "y_2").field("field3", "value3_4").endObject()));

        CountResponse countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), existsFilter("field1"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(constantScoreQuery(existsFilter("field1"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("_exists_:field1")).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), existsFilter("field2"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), existsFilter("field3"))).get();
        assertHitCount(countResponse, 1l);

        // wildcard check
        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), existsFilter("x*"))).get();
        assertHitCount(countResponse, 2l);

        // object check
        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), existsFilter("obj1"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(constantScoreQuery(missingFilter("field1"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("_missing_:field1")).get();
        assertHitCount(countResponse, 2l);

        // wildcard check
        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), missingFilter("x*"))).get();
        assertHitCount(countResponse, 2l);

        // object check
        countResponse = client().prepareCount().setQuery(filteredQuery(matchAllQuery(), missingFilter("obj1"))).get();
        assertHitCount(countResponse, 2l);
    }

    @Test
    public void passQueryAsJSONStringTest() throws Exception {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefresh(true).get();

        WrapperQueryBuilder wrapper = new WrapperQueryBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }");
        assertHitCount(client().prepareCount().setQuery(wrapper).get(), 1l);

        BoolQueryBuilder bool = boolQuery().must(wrapper).must(new TermQueryBuilder("field2", "value2_1"));
        assertHitCount(client().prepareCount().setQuery(bool).get(), 1l);
    }

    @Test
    public void testFiltersWithCustomCacheKey() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        refresh();

        CountResponse countResponse = client().prepareCount("test").setQuery(constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1"))).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1"))).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(constantScoreQuery(termsFilter("field1", "value1"))).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(constantScoreQuery(termsFilter("field1", "value1"))).get();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testMatchQueryNumeric() throws Exception {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("long", 1l, "double", 1.0d).get();
        client().prepareIndex("test", "type1", "2").setSource("long", 2l, "double", 2.0d).get();
        client().prepareIndex("test", "type1", "3").setSource("long", 3l, "double", 3.0d).get();
        refresh();
        CountResponse countResponse = client().prepareCount().setQuery(matchQuery("long", "1")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(matchQuery("double", "2")).get();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testMultiMatchQuery() throws Exception {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value4", "field3", "value3").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2", "field2", "value5", "field3", "value2").get();
        client().prepareIndex("test", "type1", "3").setSource("field1", "value3", "field2", "value6", "field3", "value1").get();
        refresh();

        MultiMatchQueryBuilder builder = QueryBuilders.multiMatchQuery("value1 value2 value4", "field1", "field2");
        CountResponse countResponse = client().prepareCount().setQuery(builder).get();
        assertHitCount(countResponse, 2l);

        refresh();
        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field2")
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        countResponse = client().prepareCount().setQuery(builder).get();
        assertHitCount(countResponse, 1l);

        refresh();
        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field3^1.5")
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        countResponse = client().prepareCount().setQuery(builder).get();
        assertHitCount(countResponse, 2l);

        refresh();
        builder = QueryBuilders.multiMatchQuery("value1").field("field1").field("field3", 1.5f)
                .operator(MatchQueryBuilder.Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        countResponse = client().prepareCount().setQuery(builder).get();
        assertHitCount(countResponse, 2l);

        // Test lenient
        client().prepareIndex("test", "type1", "3").setSource("field1", "value7", "field2", "value8", "field4", 5).get();
        refresh();

        builder = QueryBuilders.multiMatchQuery("value1", "field1", "field2", "field4");
        builder.lenient(true);
        countResponse = client().prepareCount().setQuery(builder).get();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testMatchQueryZeroTermsQuery() {
        assertAcked(prepareCreate("test")
            .addMapping("type1", "field1", "type=string,analyzer=classic", "field2", "type=string,analyzer=classic"));
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2").get();
        refresh();

        BoolQueryBuilder boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE));
        CountResponse countResponse = client().prepareCount().setQuery(boolQuery).get();
        assertHitCount(countResponse, 0l);

        boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        countResponse = client().prepareCount().setQuery(boolQuery).get();
        assertHitCount(countResponse, 1l);

        boolQuery = boolQuery().must(matchQuery("field1", "a").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        countResponse = client().prepareCount().setQuery(boolQuery).get();
        assertHitCount(countResponse, 2l);
    }

    @Test
    public void testMultiMatchQueryZeroTermsQuery() {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "field1", "type=string,analyzer=classic", "field2", "type=string,analyzer=classic"));
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value3", "field2", "value4").get();
        refresh();

        BoolQueryBuilder boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE))
                .must(multiMatchQuery("value1", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE)); // Fields are ORed together
        CountResponse countResponse = client().prepareCount().setQuery(boolQuery).get();
        assertHitCount(countResponse, 0l);

        boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL))
                .must(multiMatchQuery("value4", "field1", "field2").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        countResponse = client().prepareCount().setQuery(boolQuery).get();
        assertHitCount(countResponse, 1l);

        boolQuery = boolQuery().must(multiMatchQuery("a", "field1").zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL));
        countResponse = client().prepareCount().setQuery(boolQuery).get();
        assertHitCount(countResponse, 2l);
    }

    @Test
    public void testMultiMatchQueryMinShouldMatch() {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field1", new String[]{"value1", "value2", "value3"}).get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value1").get();
        refresh();

        MultiMatchQueryBuilder multiMatchQuery = multiMatchQuery("value1 value2 foo", "field1", "field2");

        multiMatchQuery.useDisMax(true);
        multiMatchQuery.minimumShouldMatch("70%");
        CountResponse countResponse = client().prepareCount().setQuery(multiMatchQuery).get();
        assertHitCount(countResponse, 1l);

        multiMatchQuery.minimumShouldMatch("30%");
        countResponse = client().prepareCount().setQuery(multiMatchQuery).get();
        assertHitCount(countResponse, 2l);

        multiMatchQuery.useDisMax(false);
        multiMatchQuery.minimumShouldMatch("70%");
        countResponse = client().prepareCount().setQuery(multiMatchQuery).get();
        assertHitCount(countResponse, 1l);

        multiMatchQuery.minimumShouldMatch("30%");
        countResponse = client().prepareCount().setQuery(multiMatchQuery).get();
        assertHitCount(countResponse, 2l);

        multiMatchQuery = multiMatchQuery("value1 value2 bar", "field1");
        multiMatchQuery.minimumShouldMatch("100%");
        countResponse = client().prepareCount().setQuery(multiMatchQuery).get();
        assertHitCount(countResponse, 0l);

        multiMatchQuery.minimumShouldMatch("70%");
        countResponse = client().prepareCount().setQuery(multiMatchQuery).get();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testFuzzyQueryString() {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).get();
        client().prepareIndex("test", "type1", "2").setSource("str", "shay", "date", "2012-02-05", "num", 20).get();
        refresh();

        CountResponse countResponse = client().prepareCount().setQuery(queryStringQuery("str:kimcy~1")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("num:11~1")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("date:2012-02-02~1d")).get();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testSpecialRangeSyntaxInQueryString() {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).get();
        client().prepareIndex("test", "type1", "2").setSource("str", "shay", "date", "2012-02-05", "num", 20).get();
        refresh();

        CountResponse countResponse = client().prepareCount().setQuery(queryStringQuery("num:>19")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("num:>20")).get();
        assertHitCount(countResponse, 0l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("num:>=20")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("num:>11")).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("num:<20")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("num:<=20")).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(queryStringQuery("+num:>11 +num:<20")).get();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testEmptyTermsFilter() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", "terms", "type=string"));
        ensureGreen();
        client().prepareIndex("test", "type", "1").setSource("term", "1").get();
        client().prepareIndex("test", "type", "2").setSource("term", "2").get();
        client().prepareIndex("test", "type", "3").setSource("term", "3").get();
        client().prepareIndex("test", "type", "4").setSource("term", "4").get();
        refresh();
        CountResponse countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("term", new String[0]))).get();
        assertHitCount(countResponse, 0l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), idsFilter())).get();
        assertHitCount(countResponse, 0l);
    }

    @Test
    public void testTermsLookupFilter() throws Exception {
        assertAcked(prepareCreate("lookup").addMapping("type", "terms", "type=string", "other", "type=string"));
        assertAcked(prepareCreate("lookup2").addMapping("type",
                jsonBuilder().startObject().startObject("type").startObject("properties")
                        .startObject("arr").startObject("properties").startObject("term").field("type", "string")
                        .endObject().endObject().endObject().endObject().endObject().endObject()));
        assertAcked(prepareCreate("test").addMapping("type", "term", "type=string"));
        ensureGreen();

        indexRandom(true, client().prepareIndex("lookup", "type", "1").setSource("terms", new String[]{"1", "3"}),
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
                client().prepareIndex("test", "type", "4").setSource("term", "4"));

        CountResponse countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))).get();
        assertHitCount(countResponse, 2l);

        // same as above, just on the _id...
        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("_id").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))).get();
        assertHitCount(countResponse, 2l);

        // another search with same parameters...
        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("1").lookupPath("terms"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("2").lookupPath("terms"))).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("3").lookupPath("terms"))
                ).get();
        assertNoFailures(countResponse);
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup").lookupType("type").lookupId("4").lookupPath("terms"))).get();
        assertHitCount(countResponse, 0l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("1").lookupPath("arr.term"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("2").lookupPath("arr.term"))).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("term").lookupIndex("lookup2").lookupType("type").lookupId("3").lookupPath("arr.term"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount("test")
                .setQuery(filteredQuery(matchAllQuery(), termsLookupFilter("not_exists").lookupIndex("lookup2").lookupType("type").lookupId("3").lookupPath("arr.term"))).get();
        assertHitCount(countResponse, 0l);
    }

    @Test
    public void testBasicFilterById() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type2", "2").setSource("field1", "value2").get();
        refresh();

        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1", "type2").ids("1", "2"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter().ids("1", "2"))).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1").ids("1", "2"))).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter().ids("1"))).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter(null).ids("1"))).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1", "type2", "type3").ids("1", "2", "3", "4"))).get();
        assertHitCount(countResponse, 2l);
    }

    @Test
    public void testBasicQueryById() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type2", "2").setSource("field1", "value2").get();
        refresh();

        CountResponse countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery("type1", "type2").ids("1", "2")).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery().ids("1")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery().ids("1", "2")).get();
        assertHitCount(countResponse, 2l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery("type1").ids("1", "2")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery().ids("1")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery(null).ids("1")).get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount().setQuery(QueryBuilders.idsQuery("type1", "type2", "type3").ids("1", "2", "3", "4")).get();
        assertHitCount(countResponse, 2l);
    }

    @Test
    public void testNumericTermsAndRanges() throws Exception {
        assertAcked(prepareCreate("test")
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

        CountResponse countResponse;
        logger.info("--> term query on 1");
        countResponse = client().prepareCount("test").setQuery(termQuery("num_byte", 1)).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termQuery("num_short", 1)).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termQuery("num_integer", 1)).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termQuery("num_long", 1)).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termQuery("num_float", 1)).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termQuery("num_double", 1)).get();
        assertHitCount(countResponse, 1l);

        logger.info("--> terms query on 1");
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_byte", new int[]{1})).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_short", new int[]{1})).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_integer", new int[]{1})).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_long", new int[]{1})).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_float", new double[]{1})).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(termsQuery("num_double", new double[]{1})).get();
        assertHitCount(countResponse, 1l);

        logger.info("--> term filter on 1");
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_byte", 1))).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_short", 1))).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_integer", 1))).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_long", 1))).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_float", 1))).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termFilter("num_double", 1))).get();
        assertHitCount(countResponse, 1l);

        logger.info("--> terms filter on 1");
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_byte", new int[]{1}))).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_short", new int[]{1}))).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_integer", new int[]{1}))).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_long", new int[]{1}))).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_float", new int[]{1}))).get();
        assertHitCount(countResponse, 1l);
        countResponse = client().prepareCount("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("num_double", new int[]{1}))).get();
        assertHitCount(countResponse, 1l);
    }

    @Test // see #2994
    public void testSimpleSpan() throws ElasticsearchException, IOException {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource("description", "foo other anything bar").get();
        client().prepareIndex("test", "test", "2").setSource("description", "foo other anything").get();
        client().prepareIndex("test", "test", "3").setSource("description", "foo other").get();
        client().prepareIndex("test", "test", "4").setSource("description", "foo").get();
        refresh();

        CountResponse response = client().prepareCount("test")
                .setQuery(QueryBuilders.spanOrQuery().clause(QueryBuilders.spanTermQuery("description", "bar"))).get();
        assertHitCount(response, 1l);

        response = client().prepareCount("test").setQuery(
                QueryBuilders.spanNearQuery()
                        .clause(QueryBuilders.spanTermQuery("description", "foo"))
                        .clause(QueryBuilders.spanTermQuery("description", "other"))
                        .slop(3)).get();
        assertHitCount(response, 3l);
    }
}
