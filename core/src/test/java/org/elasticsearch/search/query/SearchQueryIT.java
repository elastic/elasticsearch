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

package org.elasticsearch.search.query;

import org.apache.lucene.util.English;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.commonTermsQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhrasePrefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.regexpQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanMultiTermQueryBuilder;
import static org.elasticsearch.index.query.QueryBuilders.spanNearQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanNotQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanOrQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanTermQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsLookupQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.index.query.QueryBuilders.typeQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import static org.elasticsearch.index.query.QueryBuilders.wrapperQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThirdHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasScore;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SearchQueryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    @Override
    protected int maximumNumberOfShards() {
        return 7;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return Math.min(2, cluster().numDataNodes() - 1);
    }

    // see #3952
    public void testEmptyQueryString() throws ExecutionException, InterruptedException, IOException {
        createIndex("test");
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox jumps"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "quick brown"),
                client().prepareIndex("test", "type1", "3").setSource("field1", "quick"));

        assertHitCount(client().prepareSearch().setQuery(queryStringQuery("quick")).get(), 3L);
        assertHitCount(client().prepareSearch().setQuery(queryStringQuery("")).get(), 0L); // return no docs
    }

    // see https://github.com/elastic/elasticsearch/issues/3177
    public void testIssue3177() {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2").get();
        client().prepareIndex("test", "type1", "3").setSource("field1", "value3").get();
        ensureGreen();
        waitForRelocation();
        forceMerge();
        refresh();
        assertHitCount(
                client().prepareSearch()
                        .setQuery(matchAllQuery())
                        .setPostFilter(
                                boolQuery().must(
                                        matchAllQuery()).must(
                                        boolQuery().mustNot(boolQuery().must(termQuery("field1", "value1")).must(
                                                termQuery("field1", "value2"))))).get(),
                3L);
        assertHitCount(
                client().prepareSearch()
                        .setQuery(
                                boolQuery().must(
                                        boolQuery().should(termQuery("field1", "value1")).should(termQuery("field1", "value2"))
                                                .should(termQuery("field1", "value3"))).filter(
                                        boolQuery().mustNot(boolQuery().must(termQuery("field1", "value1")).must(
                                                termQuery("field1", "value2"))))).get(),
                3L);
        assertHitCount(
                client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(boolQuery().mustNot(termQuery("field1", "value3"))).get(),
                2L);
    }

    public void testIndexOptions() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "field1", "type=text,index_options=docs"));
        indexRandom(true,
                client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox"));

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchPhraseQuery("field2", "quick brown").slop(0)).get();
        assertHitCount(searchResponse, 1L);

        assertFailures(client().prepareSearch().setQuery(matchPhraseQuery("field1", "quick brown").slop(0)),
                    RestStatus.BAD_REQUEST,
                    containsString("field:[field1] was indexed without position data; cannot run PhraseQuery"));
    }

    // see #3521
    public void testConstantScoreQuery() throws Exception {
        Random random = random();
        createIndex("test");
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox"), client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox"));

        SearchResponse searchResponse = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("field1", "quick"))).get();
        assertHitCount(searchResponse, 2L);
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            assertSearchHit(searchHit, hasScore(1.0f));
        }

        searchResponse = client().prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).must(
                constantScoreQuery(matchQuery("field1", "quick")).boost(1.0f + random().nextFloat()))).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasScore(searchResponse.getHits().getAt(1).getScore()));

        client().prepareSearch("test").setQuery(constantScoreQuery(matchQuery("field1", "quick")).boost(1.0f + random().nextFloat())).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasScore(searchResponse.getHits().getAt(1).getScore()));

        searchResponse = client().prepareSearch("test").setQuery(
                constantScoreQuery(boolQuery().must(matchAllQuery()).must(
                constantScoreQuery(matchQuery("field1", "quick")).boost(1.0f + (random.nextBoolean()? 0.0f : random.nextFloat()))))).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasScore(searchResponse.getHits().getAt(1).getScore()));
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            assertSearchHit(searchHit, hasScore(1.0f));
        }

        int num = scaledRandomIntBetween(100, 200);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[num];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test_1", "type", "" + i).setSource("f", English.intToEnglish(i));
        }
        createIndex("test_1");
        indexRandom(true, builders);

        int queryRounds = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < queryRounds; i++) {
            MatchQueryBuilder matchQuery = matchQuery("f", English.intToEnglish(between(0, num)));
            searchResponse = client().prepareSearch("test_1").setQuery(constantScoreQuery(matchQuery)).setSize(num).get();
            long totalHits = searchResponse.getHits().getTotalHits();
            SearchHits hits = searchResponse.getHits();
            for (SearchHit searchHit : hits) {
                assertSearchHit(searchHit, hasScore(1.0f));
            }
            searchResponse = client().prepareSearch("test_1").setQuery(
                    boolQuery().must(matchAllQuery()).must(
                    constantScoreQuery(matchQuery).boost(1.0f + (random.nextBoolean()? 0.0f : random.nextFloat())))).setSize(num).get();
            hits = searchResponse.getHits();
            assertThat(hits.getTotalHits(), equalTo(totalHits));
            if (totalHits > 1) {
                float expected = hits.getAt(0).getScore();
                for (SearchHit searchHit : hits) {
                    assertSearchHit(searchHit, hasScore(expected));
                }
            }
        }
    }

    // see #3521
    public void testAllDocsQueryString() throws InterruptedException, ExecutionException {
        createIndex("test");
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("foo", "bar"),
                client().prepareIndex("test", "type1", "2").setSource("foo", "bar")
        );

        int iters = scaledRandomIntBetween(100, 200);
        for (int i = 0; i < iters; i++) {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(queryStringQuery("*:*^10.0").boost(10.0f)).get();
            assertHitCount(searchResponse, 2L);

            searchResponse = client().prepareSearch("test").setQuery(
                    boolQuery().must(matchAllQuery()).must(constantScoreQuery(matchAllQuery()))).get();
            assertHitCount(searchResponse, 2L);
            assertThat((double)searchResponse.getHits().getAt(0).getScore(), closeTo(2.0, 0.1));
            assertThat((double)searchResponse.getHits().getAt(1).getScore(),closeTo(2.0, 0.1));
        }
    }

    public void testCommonTermsQuery() throws Exception {

        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=text,analyzer=whitespace")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1)).get();
        indexRandom(true, client().prepareIndex("test", "type1", "3").setSource("field1", "quick lazy huge brown pidgin", "field2", "the quick lazy huge brown fox jumps over the tree"),
                client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "the quick lazy huge brown fox jumps over the tree") );


        SearchResponse searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the quick brown").cutoffFrequency(3).lowFreqOperator(Operator.OR)).get();
        assertHitCount(searchResponse, 3L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the quick brown").cutoffFrequency(3).lowFreqOperator(Operator.AND)).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        // Default
        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the quick brown").cutoffFrequency(3)).get();
        assertHitCount(searchResponse, 3L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));


        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the huge fox").lowFreqMinimumShouldMatch("2")).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("3")).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("2"));
        assertSecondHit(searchResponse, hasId("1"));

        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("4")).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        // Default
        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the lazy fox brown").cutoffFrequency(1)).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the quick brown").cutoffFrequency(3).analyzer("stop")).get();
        assertHitCount(searchResponse, 3L);
        // stop drops "the" since its a stopword
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("3"));
        assertThirdHit(searchResponse, hasId("2"));

        // try the same with match query
        searchResponse = client().prepareSearch().setQuery(matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(Operator.AND)).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(Operator.OR)).get();
        assertHitCount(searchResponse, 3L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(matchQuery("field1", "the quick brown").cutoffFrequency(3).operator(Operator.AND).analyzer("stop")).get();
        assertHitCount(searchResponse, 3L);
        // stop drops "the" since its a stopword
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("3"));
        assertThirdHit(searchResponse, hasId("2"));

        // try the same with multi match query
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery("the quick brown", "field1", "field2").cutoffFrequency(3).operator(Operator.AND)).get();
        assertHitCount(searchResponse, 3L);
        assertFirstHit(searchResponse, hasId("3"));
        assertSecondHit(searchResponse, hasId("1"));
        assertThirdHit(searchResponse, hasId("2"));
    }

    public void testCommonTermsQueryStackedTokens() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder()
                        .put(indexSettings())
                        .put(SETTING_NUMBER_OF_SHARDS,1)
                        .put("index.analysis.filter.syns.type","synonym")
                        .putList("index.analysis.filter.syns.synonyms","quick,fast")
                        .put("index.analysis.analyzer.syns.tokenizer","whitespace")
                        .put("index.analysis.analyzer.syns.filter","syns")
                        )
                .addMapping("type1", "field1", "type=text,analyzer=syns", "field2", "type=text,analyzer=syns"));

        indexRandom(true, client().prepareIndex("test", "type1", "3").setSource("field1", "quick lazy huge brown pidgin", "field2", "the quick lazy huge brown fox jumps over the tree"),
                client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "the quick lazy huge brown fox jumps over the tree") );

        SearchResponse searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the fast brown").cutoffFrequency(3).lowFreqOperator(Operator.OR)).get();
        assertHitCount(searchResponse, 3L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the fast brown").cutoffFrequency(3).lowFreqOperator(Operator.AND)).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        // Default
        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the fast brown").cutoffFrequency(3)).get();
        assertHitCount(searchResponse, 3L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));


        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the fast huge fox").lowFreqMinimumShouldMatch("3")).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the fast lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("5")).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("2"));
        assertSecondHit(searchResponse, hasId("1"));

        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the fast lazy fox brown").cutoffFrequency(1).highFreqMinimumShouldMatch("6")).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        // Default
        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the fast lazy fox brown").cutoffFrequency(1)).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(commonTermsQuery("field1", "the quick brown").cutoffFrequency(3).analyzer("stop")).get();
        assertHitCount(searchResponse, 3L);
        // stop drops "the" since its a stopword
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("3"));
        assertThirdHit(searchResponse, hasId("2"));

        // try the same with match query
        searchResponse = client().prepareSearch().setQuery(matchQuery("field1", "the fast brown").cutoffFrequency(3).operator(Operator.AND)).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(matchQuery("field1", "the fast brown").cutoffFrequency(3).operator(Operator.OR)).get();
        assertHitCount(searchResponse, 3L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(matchQuery("field1", "the fast brown").cutoffFrequency(3).operator(Operator.AND).analyzer("stop")).get();
        assertHitCount(searchResponse, 3L);
        // stop drops "the" since its a stopword
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("3"));
        assertThirdHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(matchQuery("field1", "the fast brown").cutoffFrequency(3).minimumShouldMatch("3")).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        // try the same with multi match query
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery("the fast brown", "field1", "field2").cutoffFrequency(3).operator(Operator.AND)).get();
        assertHitCount(searchResponse, 3L);
        assertFirstHit(searchResponse, hasId("3"));
        assertSecondHit(searchResponse, hasId("1"));
        assertThirdHit(searchResponse, hasId("2"));
    }

    public void testQueryStringAnalyzedWildcard() throws Exception {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryStringQuery("value*")).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("*ue*")).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("*ue_1")).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("val*e_1")).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("v?l*e?1")).get();
        assertHitCount(searchResponse, 1L);
    }

    public void testLowercaseExpandedTerms() {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("field1", "value_1", "field2", "value_2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryStringQuery("VALUE_3~1")).get();
        assertHitCount(searchResponse, 1L);
        searchResponse = client().prepareSearch().setQuery(queryStringQuery("ValUE_*")).get();
        assertHitCount(searchResponse, 1L);
        searchResponse = client().prepareSearch().setQuery(queryStringQuery("vAl*E_1")).get();
        assertHitCount(searchResponse, 1L);
        searchResponse = client().prepareSearch().setQuery(queryStringQuery("[VALUE_1 TO VALUE_3]")).get();
        assertHitCount(searchResponse, 1L);
    }

    // Issue #3540
    public void testDateRangeInQueryString() {
        //the mapping needs to be provided upfront otherwise we are not sure how many failures we get back
        //as with dynamic mappings some shards might be lacking behind and parse a different query
        assertAcked(prepareCreate("test").addMapping(
                "type", "past", "type=date", "future", "type=date"
        ));

        String aMonthAgo = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).minusMonths(1));
        String aMonthFromNow = ISODateTimeFormat.yearMonthDay().print(new DateTime(DateTimeZone.UTC).plusMonths(1));
        client().prepareIndex("test", "type", "1").setSource("past", aMonthAgo, "future", aMonthFromNow).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryStringQuery("past:[now-2M/d TO now/d]")).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("future:[now/d TO now+2M/d]")).get();
        assertHitCount(searchResponse, 1L);

        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> client().prepareSearch()
                .setQuery(queryStringQuery("future:[now/D TO now+2M/d]").lenient(false)).get());
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.toString(), containsString("unit [D] not supported for date math"));
    }

    // Issue #7880
    public void testDateRangeInQueryStringWithTimeZone_7880() {
        //the mapping needs to be provided upfront otherwise we are not sure how many failures we get back
        //as with dynamic mappings some shards might be lacking behind and parse a different query
        assertAcked(prepareCreate("test").addMapping(
                "type", "past", "type=date"
        ));

        DateTimeZone timeZone = randomDateTimeZone();
        String now = ISODateTimeFormat.dateTime().print(new DateTime(timeZone));
        logger.info(" --> Using time_zone [{}], now is [{}]", timeZone.getID(), now);
        client().prepareIndex("test", "type", "1").setSource("past", now).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryStringQuery("past:[now-1m/m TO now+1m/m]")
                .timeZone(timeZone.getID())).get();
        assertHitCount(searchResponse, 1L);
    }

    // Issue #10477
    public void testDateRangeInQueryStringWithTimeZone_10477() {
        //the mapping needs to be provided upfront otherwise we are not sure how many failures we get back
        //as with dynamic mappings some shards might be lacking behind and parse a different query
        assertAcked(prepareCreate("test").addMapping(
                "type", "past", "type=date"
        ));

        client().prepareIndex("test", "type", "1").setSource("past", "2015-04-05T23:00:00+0000").get();
        client().prepareIndex("test", "type", "2").setSource("past", "2015-04-06T00:00:00+0000").get();
        refresh();

        // Timezone set with dates
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(queryStringQuery("past:[2015-04-06T00:00:00+0200 TO 2015-04-06T23:00:00+0200]"))
                .get();
        assertHitCount(searchResponse, 2L);

        // Same timezone set with time_zone
        searchResponse = client().prepareSearch()
                .setQuery(queryStringQuery("past:[2015-04-06T00:00:00 TO 2015-04-06T23:00:00]").timeZone("+0200"))
                .get();
        assertHitCount(searchResponse, 2L);

        // We set a timezone which will give no result
        searchResponse = client().prepareSearch()
                .setQuery(queryStringQuery("past:[2015-04-06T00:00:00-0200 TO 2015-04-06T23:00:00-0200]"))
                .get();
        assertHitCount(searchResponse, 0L);

        // Same timezone set with time_zone but another timezone is set directly within dates which has the precedence
        searchResponse = client().prepareSearch()
                .setQuery(queryStringQuery("past:[2015-04-06T00:00:00-0200 TO 2015-04-06T23:00:00-0200]").timeZone("+0200"))
                .get();
        assertHitCount(searchResponse, 0L);
    }

    public void testTypeFilter() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id)));
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "2").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "3").setSource("field1", "value1"));

        assertHitCount(client().prepareSearch().setQuery(typeQuery("type1")).get(), 2L);
        assertHitCount(client().prepareSearch().setQuery(typeQuery("type2")).get(), 3L);

        assertHitCount(client().prepareSearch().setTypes("type1").setQuery(matchAllQuery()).get(), 2L);
        assertHitCount(client().prepareSearch().setTypes("type2").setQuery(matchAllQuery()).get(), 3L);

        assertHitCount(client().prepareSearch().setTypes("type1", "type2").setQuery(matchAllQuery()).get(), 5L);
    }

    public void testIdsQueryTestsIdIndexed() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test"));

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value2"),
                client().prepareIndex("test", "type1", "3").setSource("field1", "value3"));

        SearchResponse searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsQuery("type1").addIds("1", "3"))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");

        // no type
        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(idsQuery().addIds("1", "3"))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1").addIds("1", "3")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");

        // no type
        searchResponse = client().prepareSearch().setQuery(idsQuery().addIds("1", "3")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1").addIds("7", "10")).get();
        assertHitCount(searchResponse, 0L);

        // repeat..., with terms
        searchResponse = client().prepareSearch().setTypes("type1").setQuery(constantScoreQuery(termsQuery("_id", "1", "3"))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");
    }

    public void testTermIndexQuery() throws Exception {
        String[] indexNames = { "test1", "test2" };
        for (String indexName : indexNames) {
            assertAcked(client()
                    .admin()
                    .indices()
                    .prepareCreate(indexName));

            indexRandom(true, client().prepareIndex(indexName, "type1", indexName + "1").setSource("field1", "value1"));

        }

        for (String indexName : indexNames) {
            SearchResponse request = client().prepareSearch().setQuery(constantScoreQuery(termQuery("_index", indexName))).get();
            SearchResponse searchResponse = assertSearchResponse(request);
            assertHitCount(searchResponse, 1L);
            assertSearchHits(searchResponse, indexName + "1");
        }
        for (String indexName : indexNames) {
            SearchResponse request = client().prepareSearch().setQuery(constantScoreQuery(termsQuery("_index", indexName))).get();
            SearchResponse searchResponse = assertSearchResponse(request);
            assertHitCount(searchResponse, 1L);
            assertSearchHits(searchResponse, indexName + "1");
        }
        for (String indexName : indexNames) {
            SearchResponse request = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("_index", indexName))).get();
            SearchResponse searchResponse = assertSearchResponse(request);
            assertHitCount(searchResponse, 1L);
            assertSearchHits(searchResponse, indexName + "1");
        }
        {
            SearchResponse request = client().prepareSearch().setQuery(constantScoreQuery(termsQuery("_index", indexNames))).get();
            SearchResponse searchResponse = assertSearchResponse(request);
            assertHitCount(searchResponse, indexNames.length);
        }
    }

    public void testFilterExistsMissing() throws Exception {
        createIndex("test");

        indexRandom(true,
                client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x1", "x_1").field("field1", "value1_1").field("field2", "value2_1").endObject()),
                client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().startObject("obj1").field("obj1_val", "1").endObject().field("x2", "x_2").field("field1", "value1_2").endObject()),
                client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y1", "y_1").field("field2", "value2_3").endObject()),
                client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject().startObject("obj2").field("obj2_val", "1").endObject().field("y2", "y_2").field("field3", "value3_4").endObject()) );


        SearchResponse searchResponse = client().prepareSearch().setQuery(existsQuery("field1")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "2");

        searchResponse = client().prepareSearch().setQuery(constantScoreQuery(existsQuery("field1"))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "2");

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("_exists_:field1")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "2");

        searchResponse = client().prepareSearch().setQuery(existsQuery("field2")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch().setQuery(existsQuery("field3")).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("4"));

        // wildcard check
        searchResponse = client().prepareSearch().setQuery(existsQuery("x*")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "2");

        // object check
        searchResponse = client().prepareSearch().setQuery(existsQuery("obj1")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "2");
    }

    public void testPassQueryOrFilterAsJSONString() throws Exception {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").setRefreshPolicy(IMMEDIATE).get();

        WrapperQueryBuilder wrapper = new WrapperQueryBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }");
        assertHitCount(client().prepareSearch().setQuery(wrapper).get(), 1L);

        BoolQueryBuilder bool = boolQuery().must(wrapper).must(new TermQueryBuilder("field2", "value2_1"));
        assertHitCount(client().prepareSearch().setQuery(bool).get(), 1L);

        WrapperQueryBuilder wrapperFilter = wrapperQuery("{ \"term\" : { \"field1\" : \"value1_1\" } }");
        assertHitCount(client().prepareSearch().setPostFilter(wrapperFilter).get(), 1L);
    }

    public void testFiltersWithCustomCacheKey() throws Exception {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsQuery("field1", "value1"))).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsQuery("field1", "value1"))).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsQuery("field1", "value1"))).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsQuery("field1", "value1"))).get();
        assertHitCount(searchResponse, 1L);
    }

    public void testMatchQueryNumeric() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", "long", "type=long", "double", "type=double"));

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("long", 1L, "double", 1.0d),
                client().prepareIndex("test", "type1", "2").setSource("long", 2L, "double", 2.0d),
                client().prepareIndex("test", "type1", "3").setSource("long", 3L, "double", 3.0d));

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchQuery("long", "1")).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));

        searchResponse = client().prepareSearch().setQuery(matchQuery("double", "2")).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));
        expectThrows(SearchPhaseExecutionException.class, () -> client().prepareSearch().setQuery(matchQuery("double", "2 3 4")).get());
    }

    public void testMultiMatchQuery() throws Exception {
        createIndex("test");

        indexRandom(true,
                client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value4", "field3", "value3"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value2", "field2", "value5", "field3", "value2"),
                client().prepareIndex("test", "type1", "3").setSource("field1", "value3", "field2", "value6", "field3", "value1") );

        MultiMatchQueryBuilder builder = multiMatchQuery("value1 value2 value4", "field1", "field2");
        SearchResponse searchResponse = client().prepareSearch().setQuery(builder)
                .addAggregation(AggregationBuilders.terms("field1").field("field1.keyword")).get();

        assertHitCount(searchResponse, 2L);
        // this uses dismax so scores are equal and the order can be arbitrary
        assertSearchHits(searchResponse, "1", "2");

        builder.useDisMax(false);
        searchResponse = client().prepareSearch()
                .setQuery(builder)
                .get();

        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "2");

        client().admin().indices().prepareRefresh("test").get();
        builder = multiMatchQuery("value1", "field1", "field2")
                .operator(Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client().prepareSearch()
                .setQuery(builder)
                .get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));

        refresh();
        builder = multiMatchQuery("value1", "field1").field("field3", 1.5f)
                .operator(Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client().prepareSearch().setQuery(builder).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "3", "1");

        client().admin().indices().prepareRefresh("test").get();
        builder = multiMatchQuery("value1").field("field1").field("field3", 1.5f)
                .operator(Operator.AND); // Operator only applies on terms inside a field! Fields are always OR-ed together.
        searchResponse = client().prepareSearch().setQuery(builder).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "3", "1");

        // Test lenient
        client().prepareIndex("test", "type1", "3").setSource("field1", "value7", "field2", "value8", "field4", 5).get();
        refresh();

        builder = multiMatchQuery("value1", "field1", "field2", "field4");

        assertFailures(client().prepareSearch().setQuery(builder),
                RestStatus.BAD_REQUEST,
                containsString("NumberFormatException[For input string: \"value1\"]"));

        builder.lenient(true);
        searchResponse = client().prepareSearch().setQuery(builder).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
    }

    public void testMatchQueryZeroTermsQuery() {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "field1", "type=text,analyzer=classic", "field2", "type=text,analyzer=classic"));
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2").get();
        refresh();

        BoolQueryBuilder boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE));
        SearchResponse searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 0L);

        boolQuery = boolQuery()
                .must(matchQuery("field1", "a").zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL))
                .must(matchQuery("field1", "value1").zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 1L);

        boolQuery = boolQuery().must(matchQuery("field1", "a").zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 2L);
    }

    public void testMultiMatchQueryZeroTermsQuery() {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "field1", "type=text,analyzer=classic", "field2", "type=text,analyzer=classic"));
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value3", "field2", "value4").get();
        refresh();


        BoolQueryBuilder boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE))
                .must(multiMatchQuery("value1", "field1", "field2").zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE)); // Fields are ORed together
        SearchResponse searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 0L);

        boolQuery = boolQuery()
                .must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL))
                .must(multiMatchQuery("value4", "field1", "field2").zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 1L);

        boolQuery = boolQuery().must(multiMatchQuery("a", "field1").zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL));
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 2L);
    }

    public void testMultiMatchQueryMinShouldMatch() {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field1", new String[]{"value1", "value2", "value3"}).get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value1").get();
        refresh();

        MultiMatchQueryBuilder multiMatchQuery = multiMatchQuery("value1 value2 foo", "field1", "field2");

        multiMatchQuery.useDisMax(true);
        multiMatchQuery.minimumShouldMatch("70%");
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(multiMatchQuery)
                .get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));

        multiMatchQuery.minimumShouldMatch("30%");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        multiMatchQuery.useDisMax(false);
        multiMatchQuery.minimumShouldMatch("70%");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));

        multiMatchQuery.minimumShouldMatch("30%");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));

        multiMatchQuery = multiMatchQuery("value1 value2 bar", "field1");
        multiMatchQuery.minimumShouldMatch("100%");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 0L);

        multiMatchQuery.minimumShouldMatch("70%");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        // Min should match > # optional clauses returns no docs.
        multiMatchQuery = multiMatchQuery("value1 value2 value3", "field1", "field2");
        multiMatchQuery.minimumShouldMatch("4");
        searchResponse = client().prepareSearch().setQuery(multiMatchQuery).get();
        assertHitCount(searchResponse, 0L);
    }

    public void testBoolQueryMinShouldMatchBiggerThanNumberOfShouldClauses() throws IOException {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field1", new String[]{"value1", "value2", "value3"}).get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value1").get();
        refresh();

        BoolQueryBuilder boolQuery = boolQuery()
            .must(termQuery("field1", "value1"))
            .should(boolQuery()
                .should(termQuery("field1", "value1"))
                .should(termQuery("field1", "value2"))
                .minimumShouldMatch(3));
        SearchResponse searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));

        boolQuery = boolQuery()
            .must(termQuery("field1", "value1"))
            .should(boolQuery()
                .should(termQuery("field1", "value1"))
                .should(termQuery("field1", "value2"))
                .minimumShouldMatch(1))
            // Only one should clause is defined, returns no docs.
            .minimumShouldMatch(2);
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 0L);

        boolQuery = boolQuery()
            .should(termQuery("field1", "value1"))
            .should(boolQuery()
                .should(termQuery("field1", "value1"))
                .should(termQuery("field1", "value2"))
                .minimumShouldMatch(3))
            .minimumShouldMatch(1);
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));

        boolQuery = boolQuery()
            .must(termQuery("field1", "value1"))
            .must(boolQuery()
                .should(termQuery("field1", "value1"))
                .should(termQuery("field1", "value2"))
                .minimumShouldMatch(3));
        searchResponse = client().prepareSearch().setQuery(boolQuery).get();
        assertHitCount(searchResponse, 0L);
    }

    public void testFuzzyQueryString() {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).get();
        client().prepareIndex("test", "type1", "2").setSource("str", "shay", "date", "2012-02-05", "num", 20).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryStringQuery("str:kimcy~1")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
    }

    public void testQuotedQueryStringWithBoost() throws InterruptedException, ExecutionException {
        float boost = 10.0f;
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1)));
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("important", "phrase match", "less_important", "nothing important"),
                client().prepareIndex("test", "type1", "2").setSource("important", "nothing important", "less_important", "phrase match")
        );


        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(queryStringQuery("\"phrase match\"").field("important", boost).field("less_important")).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThat((double)searchResponse.getHits().getAt(0).getScore(), closeTo(boost * searchResponse.getHits().getAt(1).getScore(), .1));

        searchResponse = client().prepareSearch()
                .setQuery(queryStringQuery("\"phrase match\"").field("important", boost).field("less_important")).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThat((double)searchResponse.getHits().getAt(0).getScore(), closeTo(boost * searchResponse.getHits().getAt(1).getScore(), .1));
    }

    public void testSpecialRangeSyntaxInQueryString() {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).get();
        client().prepareIndex("test", "type1", "2").setSource("str", "shay", "date", "2012-02-05", "num", 20).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(queryStringQuery("num:>19")).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("num:>20")).get();
        assertHitCount(searchResponse, 0L);

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("num:>=20")).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("num:>11")).get();
        assertHitCount(searchResponse, 2L);

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("num:<20")).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("num:<=20")).get();
        assertHitCount(searchResponse, 2L);

        searchResponse = client().prepareSearch().setQuery(queryStringQuery("+num:>11 +num:<20")).get();
        assertHitCount(searchResponse, 1L);
    }

    public void testEmptytermsQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", "term", "type=text"));

        indexRandom(true, client().prepareIndex("test", "type", "1").setSource("term", "1"),
                client().prepareIndex("test", "type", "2").setSource("term", "2"),
                client().prepareIndex("test", "type", "3").setSource("term", "3"),
                client().prepareIndex("test", "type", "4").setSource("term", "4") );

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("term", new String[0]))).get();
        assertHitCount(searchResponse, 0L);

        searchResponse = client().prepareSearch("test").setQuery(idsQuery()).get();
        assertHitCount(searchResponse, 0L);
    }

    public void testTermsQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", "str", "type=text", "lng", "type=long", "dbl", "type=double"));

        indexRandom(true,
                client().prepareIndex("test", "type", "1").setSource("str", "1", "lng", 1L, "dbl", 1.0d),
                client().prepareIndex("test", "type", "2").setSource("str", "2", "lng", 2L, "dbl", 2.0d),
                client().prepareIndex("test", "type", "3").setSource("str", "3", "lng", 3L, "dbl", 3.0d),
                client().prepareIndex("test", "type", "4").setSource("str", "4", "lng", 4L, "dbl", 4.0d));

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("str", "1", "4"))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "4");

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("lng", new long[] {2, 3}))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "2", "3");

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("dbl", new double[]{2, 3}))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "2", "3");

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("lng", new int[] {1, 3}))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("dbl", new float[] {2, 4}))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "2", "4");

        // test partial matching
        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("str", "2", "5"))).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("dbl", new double[] {2, 5}))).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("lng", new long[] {2, 5}))).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        // test valid type, but no matching terms
        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("str", "5", "6"))).get();
        assertHitCount(searchResponse, 0L);

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("dbl", new double[] {5, 6}))).get();
        assertHitCount(searchResponse, 0L);

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(termsQuery("lng", new long[] {5, 6}))).get();
        assertHitCount(searchResponse, 0L);
    }

    public void testTermsLookupFilter() throws Exception {
        assertAcked(prepareCreate("lookup").addMapping("type", "terms","type=text", "other", "type=text"));
        assertAcked(prepareCreate("lookup2").addMapping("type",
                jsonBuilder().startObject().startObject("type").startObject("properties")
                        .startObject("arr").startObject("properties").startObject("term").field("type", "text")
                        .endObject().endObject().endObject().endObject().endObject().endObject()));
        assertAcked(prepareCreate("lookup3").addMapping("type", "_source", "enabled=false", "terms","type=text"));
        assertAcked(prepareCreate("test").addMapping("type", "term", "type=text"));

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
                client().prepareIndex("lookup3", "type", "1").setSource("terms", new String[]{"1", "3"}),
                client().prepareIndex("test", "type", "1").setSource("term", "1"),
                client().prepareIndex("test", "type", "2").setSource("term", "2"),
                client().prepareIndex("test", "type", "3").setSource("term", "3"),
                client().prepareIndex("test", "type", "4").setSource("term", "4") );

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(termsLookupQuery("term" , new TermsLookup("lookup", "type", "1", "terms"))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");

        // same as above, just on the _id...
        searchResponse = client().prepareSearch("test")
                .setQuery(termsLookupQuery("_id", new TermsLookup("lookup", "type", "1", "terms"))
                ).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");

        // another search with same parameters...
        searchResponse = client().prepareSearch("test")
                .setQuery(termsLookupQuery("term", new TermsLookup("lookup", "type", "1", "terms"))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch("test")
                .setQuery(termsLookupQuery("term", new TermsLookup("lookup", "type", "2", "terms"))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch("test")
                .setQuery(termsLookupQuery("term", new TermsLookup("lookup", "type", "3", "terms"))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "2", "4");

        searchResponse = client().prepareSearch("test")
                .setQuery(termsLookupQuery("term", new TermsLookup("lookup", "type", "4", "terms"))).get();
        assertHitCount(searchResponse, 0L);

        searchResponse = client().prepareSearch("test")
                .setQuery(termsLookupQuery("term", new TermsLookup("lookup2", "type", "1", "arr.term"))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "3");

        searchResponse = client().prepareSearch("test")
                .setQuery(termsLookupQuery("term", new TermsLookup("lookup2", "type", "2", "arr.term"))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("2"));

        searchResponse = client().prepareSearch("test")
                .setQuery(termsLookupQuery("term", new TermsLookup("lookup2", "type", "3", "arr.term"))).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "2", "4");

        searchResponse = client().prepareSearch("test")
                .setQuery(termsLookupQuery("not_exists", new TermsLookup("lookup2", "type", "3", "arr.term"))).get();
        assertHitCount(searchResponse, 0L);

        // index "lookup" type "type" id "missing" document does not exist: ignore the lookup terms
        searchResponse = client().prepareSearch("test")
            .setQuery(termsLookupQuery("term" , new TermsLookup("lookup", "type", "missing", "terms"))).get();
        assertHitCount(searchResponse, 0L);

        // index "lookup3" type "type" has the source disabled: ignore the lookup terms
        searchResponse = client().prepareSearch("test")
            .setQuery(termsLookupQuery("term" , new TermsLookup("lookup3", "type", "1", "terms"))).get();
        assertHitCount(searchResponse, 0L);
    }

    public void testBasicQueryById() throws Exception {
        assertAcked(prepareCreate("test"));

        client().prepareIndex("test", "doc", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "doc", "2").setSource("field1", "value2").get();
        client().prepareIndex("test", "doc", "3").setSource("field1", "value3").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(idsQuery("doc").addIds("1", "2")).get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(idsQuery().addIds("1")).get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery().addIds("1", "2")).get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(idsQuery(Strings.EMPTY_ARRAY).addIds("1")).get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1", "type2", "doc").addIds("1", "2", "3", "4")).get();
        assertHitCount(searchResponse, 3L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
    }

    public void testBasicQueryByIdMultiType() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id)));

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type2", "2").setSource("field1", "value2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(idsQuery("type1", "type2").addIds("1", "2")).get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(idsQuery().addIds("1")).get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery().addIds("1", "2")).get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1").addIds("1", "2")).get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery(Strings.EMPTY_ARRAY).addIds("1")).get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1", "type2", "type3").addIds("1", "2", "3", "4")).get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
    }


    public void testNumericTermsAndRanges() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1",
                        "num_byte", "type=byte", "num_short", "type=short",
                        "num_integer", "type=integer", "num_long", "type=long",
                        "num_float", "type=float", "num_double", "type=double"));

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
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_short", 1)).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_integer", 1)).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_long", 1)).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_float", 1)).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("num_double", 1)).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));

        logger.info("--> terms query on 1");
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_byte", new int[]{1})).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_short", new int[]{1})).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_integer", new int[]{1})).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_long", new int[]{1})).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_float", new double[]{1})).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(termsQuery("num_double", new double[]{1})).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));

        logger.info("--> term filter on 1");
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_byte", 1))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_short", 1))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_integer", 1))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_long", 1))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_float", 1))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_double", 1))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));

        logger.info("--> terms filter on 1");
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_byte", new int[]{1}))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_short", new int[]{1}))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_integer", new int[]{1}))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_long", new int[]{1}))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_float", new int[]{1}))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_double", new int[]{1}))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("1"));
    }

    public void testNumericRangeFilter_2826() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1",
                        "num_byte", "type=byte", "num_short", "type=short",
                        "num_integer", "type=integer", "num_long", "type=long",
                        "num_float", "type=float", "num_double", "type=double"));

        client().prepareIndex("test", "type1", "1").setSource("field1", "test1", "num_long", 1).get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "test1", "num_long", 2).get();
        client().prepareIndex("test", "type1", "3").setSource("field1", "test2", "num_long", 3).get();
        client().prepareIndex("test", "type1", "4").setSource("field1", "test2", "num_long", 4).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setPostFilter(
                boolQuery()
                        .should(rangeQuery("num_long").from(1).to(2))
                        .should(rangeQuery("num_long").from(3).to(4))
        ).get();
        assertHitCount(searchResponse, 4L);

        // This made 2826 fail! (only with bit based filters)
        searchResponse = client().prepareSearch("test").setPostFilter(
                boolQuery()
                        .should(rangeQuery("num_long").from(1).to(2))
                        .should(rangeQuery("num_long").from(3).to(4))
        ).get();
        assertHitCount(searchResponse, 4L);

        // This made #2979 fail!
        searchResponse = client().prepareSearch("test").setPostFilter(
                boolQuery()
                        .must(termQuery("field1", "test1"))
                        .should(rangeQuery("num_long").from(1).to(2))
                        .should(rangeQuery("num_long").from(3).to(4))
        ).get();
        assertHitCount(searchResponse, 2L);
    }

    // see #2926
    public void testMustNot() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test")
                //issue manifested only with shards>=2
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, between(2, DEFAULT_MAX_NUM_SHARDS))));


        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("description", "foo other anything bar"),
                client().prepareIndex("test", "test", "2").setSource("description", "foo other anything"),
                client().prepareIndex("test", "test", "3").setSource("description", "foo other"),
                client().prepareIndex("test", "test", "4").setSource("description", "foo"));

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).get();
        assertHitCount(searchResponse, 4L);

        searchResponse = client().prepareSearch("test").setQuery(
                boolQuery()
                        .mustNot(matchQuery("description", "anything"))
        ).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).get();
        assertHitCount(searchResponse, 2L);
    }

    // see #2994
    public void testSimpleSpan() throws IOException, ExecutionException, InterruptedException {
        createIndex("test");


        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("description", "foo other anything bar"),
                client().prepareIndex("test", "test", "2").setSource("description", "foo other anything"),
                client().prepareIndex("test", "test", "3").setSource("description", "foo other"),
                client().prepareIndex("test", "test", "4").setSource("description", "foo"));

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(spanOrQuery(spanTermQuery("description", "bar"))).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test").setQuery(
                spanNearQuery(spanTermQuery("description", "foo"), 3)
                        .addClause(spanTermQuery("description", "other"))).get();
        assertHitCount(searchResponse, 3L);
    }

    public void testSpanMultiTermQuery() throws IOException {
        createIndex("test");

        client().prepareIndex("test", "test", "1").setSource("description", "foo other anything bar", "count", 1).get();
        client().prepareIndex("test", "test", "2").setSource("description", "foo other anything", "count", 2).get();
        client().prepareIndex("test", "test", "3").setSource("description", "foo other", "count", 3).get();
        client().prepareIndex("test", "test", "4").setSource("description", "fop", "count", 4).get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(spanOrQuery(spanMultiTermQueryBuilder(fuzzyQuery("description", "fop")))).get();
        assertHitCount(response, 4);

        response = client().prepareSearch("test")
                .setQuery(spanOrQuery(spanMultiTermQueryBuilder(prefixQuery("description", "fo")))).get();
        assertHitCount(response, 4);

        response = client().prepareSearch("test")
                .setQuery(spanOrQuery(spanMultiTermQueryBuilder(wildcardQuery("description", "oth*")))).get();
        assertHitCount(response, 3);

        response = client().prepareSearch("test")
                .setQuery(spanOrQuery(spanMultiTermQueryBuilder(QueryBuilders.rangeQuery("description").from("ffa").to("foo"))))
                .execute().actionGet();
        assertHitCount(response, 3);

        response = client().prepareSearch("test")
                .setQuery(spanOrQuery(spanMultiTermQueryBuilder(regexpQuery("description", "fo{2}")))).get();
        assertHitCount(response, 3);
    }

    public void testSpanNot() throws IOException, ExecutionException, InterruptedException {
        createIndex("test");

        client().prepareIndex("test", "test", "1").setSource("description", "the quick brown fox jumped over the lazy dog").get();
        client().prepareIndex("test", "test", "2").setSource("description", "the quick black fox leaped over the sleeping dog").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(spanNotQuery(spanNearQuery(QueryBuilders.spanTermQuery("description", "quick"), 1)
                        .addClause(QueryBuilders.spanTermQuery("description", "fox")), spanTermQuery("description", "brown"))).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
                .setQuery(spanNotQuery(spanNearQuery(QueryBuilders.spanTermQuery("description", "quick"), 1)
                        .addClause(QueryBuilders.spanTermQuery("description", "fox")), spanTermQuery("description", "sleeping")).dist(5)).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
                .setQuery(spanNotQuery(spanNearQuery(QueryBuilders.spanTermQuery("description", "quick"), 1)
                        .addClause(QueryBuilders.spanTermQuery("description", "fox")), spanTermQuery("description", "jumped")).pre(1).post(1)).get();
        assertHitCount(searchResponse, 1L);
    }

    public void testSimpleDFSQuery() throws IOException {
        assertAcked(prepareCreate("test")
            .addMapping("doc", jsonBuilder()
                .startObject()
                .startObject("doc")
                .startObject("_routing")
                .field("required", true)
                .endObject()
                .startObject("properties")
                .startObject("online")
                .field("type", "boolean")
                .endObject()
                .startObject("ts")
                .field("type", "date")
                .field("ignore_malformed", false)
                .field("format", "epoch_millis")
                .endObject()
                .startObject("bs")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject())
            );


        client().prepareIndex("test", "doc", "1").setRouting("Y").setSource("online", false, "bs", "Y", "ts",
            System.currentTimeMillis() - 100, "type", "s").get();
        client().prepareIndex("test", "doc", "2").setRouting("X").setSource("online", true, "bs", "X", "ts",
            System.currentTimeMillis() - 10000000, "type", "s").get();
        client().prepareIndex("test", "doc", "3").setRouting(randomAlphaOfLength(2))
            .setSource("online", false, "ts", System.currentTimeMillis() - 100, "type", "bs").get();
        client().prepareIndex("test", "doc", "4").setRouting(randomAlphaOfLength(2))
            .setSource("online", true, "ts", System.currentTimeMillis() - 123123, "type", "bs").get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(
                        boolQuery()
                                .must(termQuery("online", true))
                                .must(boolQuery()
                                        .should(boolQuery()
                                                .must(rangeQuery("ts").lt(System.currentTimeMillis() - (15 * 1000)))
                                                .must(termQuery("type", "bs"))
                                        )
                                        .should(boolQuery()
                                                .must(rangeQuery("ts").lt(System.currentTimeMillis() - (15 * 1000)))
                                                .must(termQuery("type", "s"))
                                        )
                                )
                )
                .setVersion(true)
                .setFrom(0).setSize(100).setExplain(true).get();
        assertNoFailures(response);
    }

    public void testMultiFieldQueryString() {
        client().prepareIndex("test", "s", "1").setSource("field1", "value1", "field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        logger.info("regular");
        assertHitCount(client().prepareSearch("test").setQuery(queryStringQuery("value1").field("field1").field("field2")).get(), 1);
        assertHitCount(client().prepareSearch("test").setQuery(queryStringQuery("field\\*:value1")).get(), 1);
        logger.info("prefix");
        assertHitCount(client().prepareSearch("test").setQuery(queryStringQuery("value*").field("field1").field("field2")).get(), 1);
        assertHitCount(client().prepareSearch("test").setQuery(queryStringQuery("field\\*:value*")).get(), 1);
        logger.info("wildcard");
        assertHitCount(client().prepareSearch("test").setQuery(queryStringQuery("v?lue*").field("field1").field("field2")).get(), 1);
        assertHitCount(client().prepareSearch("test").setQuery(queryStringQuery("field\\*:v?lue*")).get(), 1);
        logger.info("fuzzy");
        assertHitCount(client().prepareSearch("test").setQuery(queryStringQuery("value~").field("field1").field("field2")).get(), 1);
        assertHitCount(client().prepareSearch("test").setQuery(queryStringQuery("field\\*:value~")).get(), 1);
        logger.info("regexp");
        assertHitCount(client().prepareSearch("test").setQuery(queryStringQuery("/value[01]/").field("field1").field("field2")).get(), 1);
        assertHitCount(client().prepareSearch("test").setQuery(queryStringQuery("field\\*:/value[01]/")).get(), 1);
    }

    // see #3881 - for extensive description of the issue
    public void testMatchQueryWithSynonyms() throws IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(Settings.builder()
                .put(indexSettings())
                .put("index.analysis.analyzer.index.type", "custom")
                .put("index.analysis.analyzer.index.tokenizer", "standard")
                .put("index.analysis.analyzer.index.filter", "lowercase")
                .put("index.analysis.analyzer.search.type", "custom")
                .put("index.analysis.analyzer.search.tokenizer", "standard")
                .putList("index.analysis.analyzer.search.filter", "lowercase", "synonym")
                .put("index.analysis.filter.synonym.type", "synonym")
                .putList("index.analysis.filter.synonym.synonyms", "fast, quick"));
        assertAcked(builder.addMapping("test", "text", "type=text,analyzer=index,search_analyzer=search"));

        client().prepareIndex("test", "test", "1").setSource("text", "quick brown fox").get();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "quick").operator(Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "quick brown").operator(Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "fast").operator(Operator.AND)).get();
        assertHitCount(searchResponse, 1);

        client().prepareIndex("test", "test", "2").setSource("text", "fast brown fox").get();
        refresh();
        searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "quick").operator(Operator.AND)).get();
        assertHitCount(searchResponse, 2);
        searchResponse = client().prepareSearch("test").setQuery(matchQuery("text", "quick brown").operator(Operator.AND)).get();
        assertHitCount(searchResponse, 2);
    }

    public void testQueryStringWithSynonyms() throws IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(Settings.builder()
                .put(indexSettings())
                .put("index.analysis.analyzer.index.type", "custom")
                .put("index.analysis.analyzer.index.tokenizer", "standard")
                .put("index.analysis.analyzer.index.filter", "lowercase")
                .put("index.analysis.analyzer.search.type", "custom")
                .put("index.analysis.analyzer.search.tokenizer", "standard")
                .putList("index.analysis.analyzer.search.filter", "lowercase", "synonym")
                .put("index.analysis.filter.synonym.type", "synonym")
                .putList("index.analysis.filter.synonym.synonyms", "fast, quick"));
        assertAcked(builder.addMapping("test", "text", "type=text,analyzer=index,search_analyzer=search"));

        client().prepareIndex("test", "test", "1").setSource("text", "quick brown fox").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(queryStringQuery("quick").defaultField("text").defaultOperator(Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch("test").setQuery(queryStringQuery("quick brown").defaultField("text").defaultOperator(Operator.AND)).get();
        assertHitCount(searchResponse, 1);
        searchResponse = client().prepareSearch().setQuery(queryStringQuery("fast").defaultField("text").defaultOperator(Operator.AND)).get();
        assertHitCount(searchResponse, 1);

        client().prepareIndex("test", "test", "2").setSource("text", "fast brown fox").get();
        refresh();

        searchResponse = client().prepareSearch("test").setQuery(queryStringQuery("quick").defaultField("text").defaultOperator(Operator.AND)).get();
        assertHitCount(searchResponse, 2);
        searchResponse = client().prepareSearch("test").setQuery(queryStringQuery("quick brown").defaultField("text").defaultOperator(Operator.AND)).get();
        assertHitCount(searchResponse, 2);
    }

    // see #3797
    public void testMultiMatchLenientIssue3797() {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource("field1", 123, "field2", "value2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("value2", "field2").field("field1", 2).lenient(true).useDisMax(false)).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("value2", "field2").field("field1", 2).lenient(true).useDisMax(true)).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("value2").field("field2", 2).lenient(true)).get();
        assertHitCount(searchResponse, 1L);
    }

    public void testMinScore() throws ExecutionException, InterruptedException {
        createIndex("test");

        client().prepareIndex("test", "test", "1").setSource("score", 1.5).get();
        client().prepareIndex("test", "test", "2").setSource("score", 1.0).get();
        client().prepareIndex("test", "test", "3").setSource("score", 2.0).get();
        client().prepareIndex("test", "test", "4").setSource("score", 0.5).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(
                functionScoreQuery(ScoreFunctionBuilders.fieldValueFactorFunction("score").missing(1.0)).setMinScore(1.5f)).get();
        assertHitCount(searchResponse, 2);
        assertFirstHit(searchResponse, hasId("3"));
        assertSecondHit(searchResponse, hasId("1"));
    }

    public void testQueryStringWithSlopAndFields() {
        assertAcked(prepareCreate("test"));

        client().prepareIndex("test", "doc", "1").setSource("desc", "one two three", "type", "customer").get();
        client().prepareIndex("test", "doc", "2").setSource("desc", "one two three", "type", "product").get();
        refresh();
        {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.queryStringQuery("\"one two\"").defaultField("desc")).get();
            assertHitCount(searchResponse, 2);
        }
        {
            SearchResponse searchResponse = client().prepareSearch("test").setPostFilter(QueryBuilders.termQuery("type", "customer"))
                .setQuery(QueryBuilders.queryStringQuery("\"one two\"").field("desc")).get();
            assertHitCount(searchResponse, 1);
        }
        {
            SearchResponse searchResponse = client().prepareSearch("test").setPostFilter(QueryBuilders.termQuery("type", "product"))
                .setQuery(QueryBuilders.queryStringQuery("\"one three\"~5").field("desc")).get();
            assertHitCount(searchResponse, 1);
        }
        {
            SearchResponse searchResponse = client().prepareSearch("test").setPostFilter(QueryBuilders.termQuery("type", "customer"))
                .setQuery(QueryBuilders.queryStringQuery("\"one two\"").defaultField("desc")).get();
            assertHitCount(searchResponse, 1);
        }
        {
            SearchResponse searchResponse = client().prepareSearch("test").setPostFilter(QueryBuilders.termQuery("type", "customer"))
                .setQuery(QueryBuilders.queryStringQuery("\"one two\"").defaultField("desc")).get();
            assertHitCount(searchResponse, 1);
        }
    }

    public void testDateProvidedAsNumber() throws ExecutionException, InterruptedException {
        createIndex("test");
        assertAcked(client().admin().indices().preparePutMapping("test").setType("type").setSource("field", "type=date,format=epoch_millis").get());
        indexRandom(true, client().prepareIndex("test", "type", "1").setSource("field", -1000000000001L),
                client().prepareIndex("test", "type", "2").setSource("field", -1000000000000L),
                client().prepareIndex("test", "type", "3").setSource("field", -999999999999L),
                client().prepareIndex("test", "type", "4").setSource("field", -1000000000001.0123456789),
                client().prepareIndex("test", "type", "5").setSource("field", -1000000000000.0123456789),
                client().prepareIndex("test", "type", "6").setSource("field", -999999999999.0123456789));


        assertHitCount(client().prepareSearch("test").setSize(0).setQuery(rangeQuery("field").lte(-1000000000000L)).get(), 4);
        assertHitCount(client().prepareSearch("test").setSize(0).setQuery(rangeQuery("field").lte(-999999999999L)).get(), 6);

    }

    public void testRangeQueryWithTimeZone() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "date", "type=date", "num", "type=integer"));

        indexRandom(true,
                client().prepareIndex("test", "type1", "1").setSource("date", "2014-01-01", "num", 1),
                client().prepareIndex("test", "type1", "2").setSource("date", "2013-12-31T23:00:00", "num", 2),
                client().prepareIndex("test", "type1", "3").setSource("date", "2014-01-01T01:00:00", "num", 3),
                // Now in UTC+1
                client().prepareIndex("test", "type1", "4").setSource("date", DateTime.now(DateTimeZone.forOffsetHours(1)).getMillis(), "num", 4));

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("2014-01-01T00:00:00").to("2014-01-01T00:59:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("1"));
        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("2013-12-31T23:00:00").to("2013-12-31T23:59:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("2"));
        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("2014-01-01T01:00:00").to("2014-01-01T01:59:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("3"));

        // We explicitly define a time zone in the from/to dates so whatever the time zone is, it won't be used
        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("2014-01-01T00:00:00Z").to("2014-01-01T00:59:00Z").timeZone("+10:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("1"));
        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("2013-12-31T23:00:00Z").to("2013-12-31T23:59:00Z").timeZone("+10:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("2"));
        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("2014-01-01T01:00:00Z").to("2014-01-01T01:59:00Z").timeZone("+10:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("3"));

        // We define a time zone to be applied to the filter and from/to have no time zone
        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("2014-01-01T03:00:00").to("2014-01-01T03:59:00").timeZone("+03:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("1"));
        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("2014-01-01T02:00:00").to("2014-01-01T02:59:00").timeZone("+03:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("2"));
        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("2014-01-01T04:00:00").to("2014-01-01T04:59:00").timeZone("+03:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("3"));

        // When we use long values, it means we have ms since epoch UTC based so we don't apply any transformation
        Exception e = expectThrows(SearchPhaseExecutionException.class, () ->
            client().prepareSearch("test")
                    .setQuery(QueryBuilders.rangeQuery("date").from(1388534400000L).to(1388537940999L).timeZone("+01:00"))
                    .get());

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("2014-01-01").to("2014-01-01T00:59:00").timeZone("-01:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("3"));

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("date").from("now/d-1d").timeZone("+01:00"))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), is("4"));
    }

    public void testSearchEmptyDoc() {
        assertAcked(prepareCreate("test").setSettings("{\"index.analysis.analyzer.default.type\":\"keyword\"}", XContentType.JSON));
        client().prepareIndex("test", "type1", "1").setSource("{}", XContentType.JSON).get();

        refresh();
        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
    }

    // see #5120
    public void testNGramCopyField() {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(Settings.builder()
                .put(indexSettings())
                .put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 9)
                .put("index.analysis.analyzer.my_ngram_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_ngram_analyzer.tokenizer", "my_ngram_tokenizer")
                .put("index.analysis.tokenizer.my_ngram_tokenizer.type", "nGram")
                .put("index.analysis.tokenizer.my_ngram_tokenizer.min_gram", "1")
                .put("index.analysis.tokenizer.my_ngram_tokenizer.max_gram", "10")
                .putList("index.analysis.tokenizer.my_ngram_tokenizer.token_chars", new String[0]));
        assertAcked(builder.addMapping("test", "origin", "type=text,copy_to=meta", "meta", "type=text,analyzer=my_ngram_analyzer"));
        // we only have ngrams as the index analyzer so searches will get standard analyzer


        client().prepareIndex("test", "test", "1").setSource("origin", "C.A1234.5678")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(matchQuery("meta", "1234"))
                .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
                .setQuery(matchQuery("meta", "1234.56"))
                .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
                .setQuery(termQuery("meta", "A1234"))
                .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
                .setQuery(termQuery("meta", "a1234"))
                .get();
        assertHitCount(searchResponse, 0L); // it's upper case

        searchResponse = client().prepareSearch("test")
                .setQuery(matchQuery("meta", "A1234").analyzer("my_ngram_analyzer"))
                .get(); // force ngram analyzer
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
                .setQuery(matchQuery("meta", "a1234").analyzer("my_ngram_analyzer"))
                .get(); // this one returns a hit since it's default operator is OR
        assertHitCount(searchResponse, 1L);
    }

    public void testMatchPhrasePrefixQuery() throws ExecutionException, InterruptedException {
        createIndex("test1");
        indexRandom(true, client().prepareIndex("test1", "type1", "1").setSource("field", "Johnnie Walker Black Label"),
        client().prepareIndex("test1", "type1", "2").setSource("field", "trying out Elasticsearch"));


        SearchResponse searchResponse = client().prepareSearch().setQuery(matchPhrasePrefixQuery("field", "Johnnie la").slop(between(2, 5)))
                .get();
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");
        searchResponse = client().prepareSearch().setQuery(matchPhrasePrefixQuery("field", "trying")).get();
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "2");
        searchResponse = client().prepareSearch().setQuery(matchPhrasePrefixQuery("field", "try")).get();
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "2");
    }

    public void testQueryStringParserCache() throws Exception {
        createIndex("test");
        indexRandom(true, false, client().prepareIndex("test", "type", "1").setSource("nameTokens", "xyz"));


        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.queryStringQuery("xyz").boost(100))
                .get();
        assertThat(response.getHits().getTotalHits(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));

        float first = response.getHits().getAt(0).getScore();
        for (int i = 0; i < 100; i++) {
            response = client().prepareSearch("test")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.queryStringQuery("xyz").boost(100))
                    .get();

            assertThat(response.getHits().getTotalHits(), equalTo(1L));
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            float actual = response.getHits().getAt(0).getScore();
            assertThat(i + " expected: " + first + " actual: " + actual, Float.compare(first, actual), equalTo(0));
        }
    }

}
