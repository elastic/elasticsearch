/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.analysis.pattern.PatternReplaceCharFilter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.English;
import org.apache.lucene.util.AttributeSource;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.search.SpanBooleanQueryRewriteWithMaxClause;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.NormalizingCharFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.index.query.ZeroTermsQueryOption;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.Reader;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
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
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import static org.elasticsearch.index.query.QueryBuilders.wrapperQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponses;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasScore;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class SearchQueryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, MockAnalysisPlugin.class);
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
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field1", "the quick brown fox jumps"),
            prepareIndex("test").setId("2").setSource("field1", "quick brown"),
            prepareIndex("test").setId("3").setSource("field1", "quick")
        );

        assertHitCount(prepareSearch().setQuery(queryStringQuery("quick")), 3L);
        assertHitCount(prepareSearch().setQuery(queryStringQuery("")), 0L); // return no docs
    }

    // see https://github.com/elastic/elasticsearch/issues/3177
    public void testIssue3177() {
        createIndex("test");
        prepareIndex("test").setId("1").setSource("field1", "value1").get();
        prepareIndex("test").setId("2").setSource("field1", "value2").get();
        prepareIndex("test").setId("3").setSource("field1", "value3").get();
        ensureGreen();
        waitForRelocation();
        forceMerge();
        refresh();
        assertHitCount(
            3L,
            prepareSearch().setQuery(matchAllQuery())
                .setPostFilter(
                    boolQuery().must(matchAllQuery())
                        .must(boolQuery().mustNot(boolQuery().must(termQuery("field1", "value1")).must(termQuery("field1", "value2"))))
                ),
            prepareSearch().setQuery(
                boolQuery().must(
                    boolQuery().should(termQuery("field1", "value1"))
                        .should(termQuery("field1", "value2"))
                        .should(termQuery("field1", "value3"))
                ).filter(boolQuery().mustNot(boolQuery().must(termQuery("field1", "value1")).must(termQuery("field1", "value2"))))
            )
        );
        assertHitCount(prepareSearch().setQuery(matchAllQuery()).setPostFilter(boolQuery().mustNot(termQuery("field1", "value3"))), 2L);
    }

    public void testIndexOptions() throws Exception {
        assertAcked(prepareCreate("test").setMapping("field1", "type=text,index_options=docs"));
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field1", "quick brown fox", "field2", "quick brown fox"),
            prepareIndex("test").setId("2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox")
        );

        assertHitCount(prepareSearch().setQuery(matchPhraseQuery("field2", "quick brown").slop(0)), 1L);

        assertFailures(
            prepareSearch().setQuery(matchPhraseQuery("field1", "quick brown").slop(0)),
            RestStatus.BAD_REQUEST,
            containsString("field:[field1] was indexed without position data; cannot run PhraseQuery")
        );
    }

    // see #3521
    public void testConstantScoreQuery() throws Exception {
        Random random = random();
        createIndex("test");
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field1", "quick brown fox", "field2", "quick brown fox"),
            prepareIndex("test").setId("2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox")
        );

        assertResponse(prepareSearch().setQuery(constantScoreQuery(matchQuery("field1", "quick"))), response -> {
            assertHitCount(response, 2L);
            for (SearchHit searchHit : response.getHits().getHits()) {
                assertThat(searchHit, hasScore(1.0f));
            }
        });
        assertResponses(response -> {
            assertHitCount(response, 2L);
            assertFirstHit(response, hasScore(response.getHits().getAt(1).getScore()));
        },
            prepareSearch("test").setQuery(constantScoreQuery(matchQuery("field1", "quick")).boost(1.0f + random().nextFloat())),
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).must(constantScoreQuery(matchQuery("field1", "quick")).boost(1.0f + random().nextFloat()))
            )
        );
        assertResponse(
            prepareSearch("test").setQuery(
                constantScoreQuery(
                    boolQuery().must(matchAllQuery())
                        .must(
                            constantScoreQuery(matchQuery("field1", "quick")).boost(
                                1.0f + (random.nextBoolean() ? 0.0f : random.nextFloat())
                            )
                        )
                )
            ),
            response -> {
                assertHitCount(response, 2L);
                assertFirstHit(response, hasScore(response.getHits().getAt(1).getScore()));
                for (SearchHit searchHit : response.getHits().getHits()) {
                    assertThat(searchHit, hasScore(1.0f));
                }
            }
        );
        int num = scaledRandomIntBetween(100, 200);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[num];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex("test_1").setId("" + i).setSource("f", English.intToEnglish(i));
        }
        createIndex("test_1");
        indexRandom(true, builders);

        int queryRounds = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < queryRounds; i++) {
            MatchQueryBuilder matchQuery = matchQuery("f", English.intToEnglish(between(0, num)));
            final long[] constantScoreTotalHits = new long[1];
            assertResponse(prepareSearch("test_1").setQuery(constantScoreQuery(matchQuery)).setSize(num), response -> {
                constantScoreTotalHits[0] = response.getHits().getTotalHits().value();
                SearchHits hits = response.getHits();
                for (SearchHit searchHit : hits) {
                    assertThat(searchHit, hasScore(1.0f));
                }
            });
            assertResponse(
                prepareSearch("test_1").setQuery(
                    boolQuery().must(matchAllQuery())
                        .must(constantScoreQuery(matchQuery).boost(1.0f + (random.nextBoolean() ? 0.0f : random.nextFloat())))
                ).setSize(num),
                response -> {
                    SearchHits hits = response.getHits();
                    assertThat(hits.getTotalHits().value(), equalTo(constantScoreTotalHits[0]));
                    if (constantScoreTotalHits[0] > 1) {
                        float expected = hits.getAt(0).getScore();
                        for (SearchHit searchHit : hits) {
                            assertThat(searchHit, hasScore(expected));
                        }
                    }
                }
            );
        }
    }

    // see #3521
    public void testAllDocsQueryString() throws InterruptedException, ExecutionException {
        createIndex("test");
        indexRandom(true, prepareIndex("test").setId("1").setSource("foo", "bar"), prepareIndex("test").setId("2").setSource("foo", "bar"));

        int iters = scaledRandomIntBetween(100, 200);
        for (int i = 0; i < iters; i++) {
            assertHitCount(prepareSearch("test").setQuery(queryStringQuery("*:*^10.0").boost(10.0f)), 2L);

            assertResponse(
                prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).must(constantScoreQuery(matchAllQuery()))),
                response -> {
                    assertHitCount(response, 2L);
                    assertThat((double) response.getHits().getAt(0).getScore(), closeTo(2.0, 0.1));
                    assertThat((double) response.getHits().getAt(1).getScore(), closeTo(2.0, 0.1));
                }
            );
        }
    }

    public void testQueryStringAnalyzedWildcard() throws Exception {
        createIndex("test");

        prepareIndex("test").setId("1").setSource("field1", "value_1", "field2", "value_2").get();
        refresh();

        assertHitCount(
            1L,
            prepareSearch().setQuery(queryStringQuery("value*")),
            prepareSearch().setQuery(queryStringQuery("*ue*")),
            prepareSearch().setQuery(queryStringQuery("*ue_1")),
            prepareSearch().setQuery(queryStringQuery("val*e_1")),
            prepareSearch().setQuery(queryStringQuery("v?l*e?1"))
        );
    }

    public void testLowercaseExpandedTerms() {
        createIndex("test");

        prepareIndex("test").setId("1").setSource("field1", "value_1", "field2", "value_2").get();
        refresh();

        assertHitCount(
            1L,
            prepareSearch().setQuery(queryStringQuery("VALUE_3~1")),
            prepareSearch().setQuery(queryStringQuery("ValUE_*")),
            prepareSearch().setQuery(queryStringQuery("vAl*E_1")),
            prepareSearch().setQuery(queryStringQuery("[VALUE_1 TO VALUE_3]"))
        );
    }

    // Issue #3540
    public void testDateRangeInQueryString() {
        // the mapping needs to be provided upfront otherwise we are not sure how many failures we get back
        // as with dynamic mappings some shards might be lacking behind and parse a different query
        assertAcked(prepareCreate("test").setMapping("past", "type=date", "future", "type=date"));

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String aMonthAgo = DateTimeFormatter.ISO_LOCAL_DATE.format(now.minusMonths(1));
        String aMonthFromNow = DateTimeFormatter.ISO_LOCAL_DATE.format(now.plusMonths(1));
        prepareIndex("test").setId("1").setSource("past", aMonthAgo, "future", aMonthFromNow).get();
        refresh();

        assertHitCount(
            1L,
            prepareSearch().setQuery(queryStringQuery("past:[now-2M/d TO now/d]")),
            prepareSearch().setQuery(queryStringQuery("future:[now/d TO now+2M/d]"))
        );

        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            prepareSearch().setQuery(queryStringQuery("future:[now/D TO now+2M/d]").lenient(false))
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.toString(), containsString("unit [D] not supported for date math"));
    }

    // Issue #7880
    public void testDateRangeInQueryStringWithTimeZone_7880() {
        // the mapping needs to be provided upfront otherwise we are not sure how many failures we get back
        // as with dynamic mappings some shards might be lacking behind and parse a different query
        assertAcked(prepareCreate("test").setMapping("past", "type=date"));

        ZoneId timeZone = randomZone();
        String now = DateFormatter.forPattern("strict_date_optional_time").format(Instant.now().atZone(timeZone));
        logger.info(" --> Using time_zone [{}], now is [{}]", timeZone.getId(), now);
        prepareIndex("test").setId("1").setSource("past", now).get();
        refresh();

        assertHitCount(prepareSearch().setQuery(queryStringQuery("past:[now-1m/m TO now+1m/m]").timeZone(timeZone.getId())), 1L);
    }

    // Issue #10477
    public void testDateRangeInQueryStringWithTimeZone_10477() {
        // the mapping needs to be provided upfront otherwise we are not sure how many failures we get back
        // as with dynamic mappings some shards might be lacking behind and parse a different query
        assertAcked(prepareCreate("test").setMapping("past", "type=date"));

        prepareIndex("test").setId("1").setSource("past", "2015-04-05T23:00:00+0000").get();
        prepareIndex("test").setId("2").setSource("past", "2015-04-06T00:00:00+0000").get();
        refresh();

        // Timezone set with dates
        assertHitCount(
            2L,
            prepareSearch().setQuery(queryStringQuery("past:[2015-04-06T00:00:00+0200 TO 2015-04-06T23:00:00+0200]")),
            prepareSearch().setQuery(queryStringQuery("past:[2015-04-06T00:00:00 TO 2015-04-06T23:00:00]").timeZone("+0200"))
        );

        // We set a timezone which will give no result
        assertHitCount(
            0L,
            prepareSearch().setQuery(queryStringQuery("past:[2015-04-06T00:00:00-0200 TO 2015-04-06T23:00:00-0200]")),
            prepareSearch().setQuery(queryStringQuery("past:[2015-04-06T00:00:00-0200 TO 2015-04-06T23:00:00-0200]").timeZone("+0200"))
        );
    }

    public void testIdsQueryTestsIdIndexed() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test"));

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field1", "value1"),
            prepareIndex("test").setId("2").setSource("field1", "value2"),
            prepareIndex("test").setId("3").setSource("field1", "value3")
        );

        assertSearchHitsWithoutFailures(prepareSearch().setQuery(constantScoreQuery(idsQuery().addIds("1", "3"))), "1", "3");
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(idsQuery().addIds("1", "3")), "1", "3");

        assertHitCount(prepareSearch().setQuery(idsQuery().addIds("7", "10")), 0L);

        // repeat..., with terms
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(constantScoreQuery(termsQuery("_id", "1", "3"))), "1", "3");
    }

    public void testTermIndexQuery() throws Exception {
        String[] indexNames = { "test1", "test2" };
        for (String indexName : indexNames) {
            assertAcked(indicesAdmin().prepareCreate(indexName));

            indexRandom(true, prepareIndex(indexName).setId(indexName + "1").setSource("field1", "value1"));

        }

        for (String indexName : indexNames) {
            assertSearchHitsWithoutFailures(prepareSearch().setQuery(constantScoreQuery(termQuery("_index", indexName))), indexName + "1");
        }
        for (String indexName : indexNames) {
            assertSearchHitsWithoutFailures(prepareSearch().setQuery(constantScoreQuery(termsQuery("_index", indexName))), indexName + "1");
        }
        for (String indexName : indexNames) {
            assertSearchHitsWithoutFailures(prepareSearch().setQuery(constantScoreQuery(matchQuery("_index", indexName))), indexName + "1");
        }
        {
            assertHitCountAndNoFailures(prepareSearch().setQuery(constantScoreQuery(termsQuery("_index", indexNames))), indexNames.length);
        }
    }

    public void testFilterExistsMissing() throws Exception {
        createIndex("test");

        indexRandom(
            true,
            prepareIndex("test").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .startObject("obj1")
                        .field("obj1_val", "1")
                        .endObject()
                        .field("x1", "x_1")
                        .field("field1", "value1_1")
                        .field("field2", "value2_1")
                        .endObject()
                ),
            prepareIndex("test").setId("2")
                .setSource(
                    jsonBuilder().startObject()
                        .startObject("obj1")
                        .field("obj1_val", "1")
                        .endObject()
                        .field("x2", "x_2")
                        .field("field1", "value1_2")
                        .endObject()
                ),
            prepareIndex("test").setId("3")
                .setSource(
                    jsonBuilder().startObject()
                        .startObject("obj2")
                        .field("obj2_val", "1")
                        .endObject()
                        .field("y1", "y_1")
                        .field("field2", "value2_3")
                        .endObject()
                ),
            prepareIndex("test").setId("4")
                .setSource(
                    jsonBuilder().startObject()
                        .startObject("obj2")
                        .field("obj2_val", "1")
                        .endObject()
                        .field("y2", "y_2")
                        .field("field3", "value3_4")
                        .endObject()
                )
        );

        assertSearchHitsWithoutFailures(prepareSearch().setQuery(existsQuery("field1")), "1", "2");
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(constantScoreQuery(existsQuery("field1"))), "1", "2");
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(queryStringQuery("_exists_:field1")), "1", "2");
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(existsQuery("field2")), "1", "3");
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(existsQuery("field3")), "4");
        // wildcard check
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(existsQuery("x*")), "1", "2");
        // object check
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(existsQuery("obj1")), "1", "2");
    }

    public void testPassQueryOrFilterAsJSONString() throws Exception {
        createIndex("test");

        prepareIndex("test").setId("1").setSource("field1", "value1_1", "field2", "value2_1").setRefreshPolicy(IMMEDIATE).get();

        WrapperQueryBuilder wrapper = new WrapperQueryBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }");
        assertHitCount(
            1L,
            prepareSearch().setQuery(wrapper),
            prepareSearch().setQuery(boolQuery().must(wrapper).must(new TermQueryBuilder("field2", "value2_1"))),
            prepareSearch().setPostFilter(wrapperQuery("{ \"term\" : { \"field1\" : \"value1_1\" } }"))
        );
    }

    public void testFiltersWithCustomCacheKey() throws Exception {
        createIndex("test");

        prepareIndex("test").setId("1").setSource("field1", "value1").get();
        refresh();
        assertHitCount(
            1L,
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("field1", "value1"))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("field1", "value1"))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("field1", "value1"))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("field1", "value1")))
        );
    }

    public void testMatchQueryNumeric() throws Exception {
        assertAcked(prepareCreate("test").setMapping("long", "type=long", "double", "type=double"));

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("long", 1L, "double", 1.0d),
            prepareIndex("test").setId("2").setSource("long", 2L, "double", 2.0d),
            prepareIndex("test").setId("3").setSource("long", 3L, "double", 3.0d)
        );

        assertResponse(prepareSearch().setQuery(matchQuery("long", "1")), response -> {
            assertHitCount(response, 1L);
            assertFirstHit(response, hasId("1"));
        });
        assertResponse(prepareSearch().setQuery(matchQuery("double", "2")), response -> {
            assertHitCount(response, 1L);
            assertFirstHit(response, hasId("2"));
            expectThrows(SearchPhaseExecutionException.class, prepareSearch().setQuery(matchQuery("double", "2 3 4")));
        });
    }

    public void testMatchQueryFuzzy() throws Exception {
        assertAcked(prepareCreate("test").setMapping("text", "type=text"));

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("text", "Unit"),
            prepareIndex("test").setId("2").setSource("text", "Unity")
        );
        assertHitCount(prepareSearch().setQuery(matchQuery("text", "uniy").fuzziness(Fuzziness.fromEdits(0))), 0L);

        assertSearchHitsWithoutFailures(prepareSearch().setQuery(matchQuery("text", "uniy").fuzziness(Fuzziness.fromEdits(1))), "1", "2");
        assertSearchHitsWithoutFailures(
            prepareSearch().setQuery(matchQuery("text", "uniy").fuzziness(Fuzziness.fromString("AUTO"))),
            "1",
            "2"
        );

        assertHitCount(prepareSearch().setQuery(matchQuery("text", "uniy").fuzziness(Fuzziness.fromString("AUTO:5,7"))), 0L);
        assertSearchHitsWithoutFailures(
            prepareSearch().setQuery(matchQuery("text", "unify").fuzziness(Fuzziness.fromString("AUTO:5,7"))),
            "2"
        );
    }

    public void testMultiMatchQuery() throws Exception {
        createIndex("test");

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value4", "field3", "value3"),
            prepareIndex("test").setId("2").setSource("field1", "value2", "field2", "value5", "field3", "value2"),
            prepareIndex("test").setId("3").setSource("field1", "value3", "field2", "value6", "field3", "value1")
        );

        MultiMatchQueryBuilder builder = multiMatchQuery("value1 value2 value4", "field1", "field2");
        // this uses dismax so scores are equal and the order can be arbitrary
        assertSearchHitsWithoutFailures(
            prepareSearch().setQuery(builder).addAggregation(AggregationBuilders.terms("field1").field("field1.keyword")),
            "1",
            "2"
        );

        assertSearchHitsWithoutFailures(prepareSearch().setQuery(builder), "1", "2");

        indicesAdmin().prepareRefresh("test").get();
        builder = multiMatchQuery("value1", "field1", "field2").operator(Operator.AND); // Operator only applies on terms inside a field!
        // Fields are always OR-ed together.
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(builder), "1");

        refresh();
        builder = multiMatchQuery("value1", "field1").field("field3", 1.5f).operator(Operator.AND); // Operator only applies on terms inside
        // a field! Fields are always OR-ed
        // together.
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(builder), "3", "1");

        indicesAdmin().prepareRefresh("test").get();
        builder = multiMatchQuery("value1").field("field1").field("field3", 1.5f).operator(Operator.AND); // Operator only applies on terms
        // inside a field! Fields are
        // always OR-ed together.
        assertResponse(prepareSearch().setQuery(builder), response -> {
            assertHitCount(response, 2L);
            assertSearchHits(response, "3", "1");
        });
        // Test lenient
        prepareIndex("test").setId("3").setSource("field1", "value7", "field2", "value8", "field4", 5).get();
        refresh();

        builder = multiMatchQuery("value1", "field1", "field2", "field4");

        // when the number for shards is randomized and we expect failures
        // we can either run into partial or total failures depending on the current number of shards
        Matcher<String> reasonMatcher = containsString("NumberFormatException: For input string: \"value1\"");
        try {
            assertResponse(prepareSearch().setQuery(builder), response -> {
                ShardSearchFailure[] shardFailures = response.getShardFailures();
                assertThat("Expected shard failures, got none", shardFailures, not(emptyArray()));
                for (ShardSearchFailure shardSearchFailure : shardFailures) {
                    assertThat(shardSearchFailure.status(), equalTo(RestStatus.BAD_REQUEST));
                    assertThat(shardSearchFailure.reason(), reasonMatcher);
                }
            });

        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
            ShardSearchFailure[] shardFailures = e.shardFailures();
            for (ShardSearchFailure shardSearchFailure : shardFailures) {
                assertThat(shardSearchFailure.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(shardSearchFailure.reason(), reasonMatcher);
            }
        }

        builder.lenient(true);
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(builder), "1");
    }

    public void testMatchQueryZeroTermsQuery() {
        assertAcked(prepareCreate("test").setMapping("field1", "type=text,analyzer=classic", "field2", "type=text,analyzer=classic"));
        prepareIndex("test").setId("1").setSource("field1", "value1").get();
        prepareIndex("test").setId("2").setSource("field1", "value2").get();
        refresh();

        BoolQueryBuilder boolQuery = boolQuery().must(matchQuery("field1", "a").zeroTermsQuery(ZeroTermsQueryOption.NONE))
            .must(matchQuery("field1", "value1").zeroTermsQuery(ZeroTermsQueryOption.NONE));
        assertHitCount(prepareSearch().setQuery(boolQuery), 0L);

        boolQuery = boolQuery().must(matchQuery("field1", "a").zeroTermsQuery(ZeroTermsQueryOption.ALL))
            .must(matchQuery("field1", "value1").zeroTermsQuery(ZeroTermsQueryOption.ALL));
        assertHitCount(prepareSearch().setQuery(boolQuery), 1L);

        boolQuery = boolQuery().must(matchQuery("field1", "a").zeroTermsQuery(ZeroTermsQueryOption.ALL));
        assertHitCount(prepareSearch().setQuery(boolQuery), 2L);
    }

    public void testMultiMatchQueryZeroTermsQuery() {
        assertAcked(prepareCreate("test").setMapping("field1", "type=text,analyzer=classic", "field2", "type=text,analyzer=classic"));
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").get();
        prepareIndex("test").setId("2").setSource("field1", "value3", "field2", "value4").get();
        refresh();

        BoolQueryBuilder boolQuery = boolQuery().must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(ZeroTermsQueryOption.NONE))
            // Fields are ORed together
            .must(multiMatchQuery("value1", "field1", "field2").zeroTermsQuery(ZeroTermsQueryOption.NONE));
        assertHitCount(prepareSearch().setQuery(boolQuery), 0L);

        boolQuery = boolQuery().must(multiMatchQuery("a", "field1", "field2").zeroTermsQuery(ZeroTermsQueryOption.ALL))
            .must(multiMatchQuery("value4", "field1", "field2").zeroTermsQuery(ZeroTermsQueryOption.ALL));
        assertHitCount(prepareSearch().setQuery(boolQuery), 1L);

        boolQuery = boolQuery().must(multiMatchQuery("a", "field1").zeroTermsQuery(ZeroTermsQueryOption.ALL));
        assertHitCount(prepareSearch().setQuery(boolQuery), 2L);
    }

    public void testMultiMatchQueryMinShouldMatch() {
        createIndex("test");
        prepareIndex("test").setId("1").setSource("field1", new String[] { "value1", "value2", "value3" }).get();
        prepareIndex("test").setId("2").setSource("field2", "value1").get();
        refresh();

        MultiMatchQueryBuilder multiMatchQuery = multiMatchQuery("value1 value2 foo", "field1", "field2");

        multiMatchQuery.minimumShouldMatch("70%");
        assertResponse(prepareSearch().setQuery(multiMatchQuery), response -> {
            assertHitCount(response, 1L);
            assertFirstHit(response, hasId("1"));
        });
        multiMatchQuery.minimumShouldMatch("30%");
        assertResponse(prepareSearch().setQuery(multiMatchQuery), response -> {
            assertHitCount(response, 2L);
            assertFirstHit(response, hasId("1"));
            assertSecondHit(response, hasId("2"));
        });
        multiMatchQuery.minimumShouldMatch("70%");
        assertResponse(prepareSearch().setQuery(multiMatchQuery), response -> {
            assertHitCount(response, 1L);
            assertFirstHit(response, hasId("1"));
        });
        multiMatchQuery.minimumShouldMatch("30%");
        assertResponse(prepareSearch().setQuery(multiMatchQuery), response -> {
            assertHitCount(response, 2L);
            assertFirstHit(response, hasId("1"));
            assertSecondHit(response, hasId("2"));
        });
        multiMatchQuery = multiMatchQuery("value1 value2 bar", "field1");
        multiMatchQuery.minimumShouldMatch("100%");
        assertHitCount(prepareSearch().setQuery(multiMatchQuery), 0L);

        multiMatchQuery.minimumShouldMatch("70%");
        assertResponse(prepareSearch().setQuery(multiMatchQuery), response -> {
            assertHitCount(response, 1L);
            assertFirstHit(response, hasId("1"));
        });
        // Min should match > # optional clauses returns no docs.
        multiMatchQuery = multiMatchQuery("value1 value2 value3", "field1", "field2");
        multiMatchQuery.minimumShouldMatch("4");
        assertHitCount(prepareSearch().setQuery(multiMatchQuery), 0L);
    }

    public void testBoolQueryMinShouldMatchBiggerThanNumberOfShouldClauses() throws IOException {
        createIndex("test");
        prepareIndex("test").setId("1").setSource("field1", new String[] { "value1", "value2", "value3" }).get();
        prepareIndex("test").setId("2").setSource("field2", "value1").get();
        refresh();

        assertResponses(response -> {
            assertHitCount(response, 1L);
            assertFirstHit(response, hasId("1"));
        },
            prepareSearch().setQuery(
                boolQuery().must(termQuery("field1", "value1"))
                    .should(boolQuery().should(termQuery("field1", "value1")).should(termQuery("field1", "value2")).minimumShouldMatch(3))
            ),
            prepareSearch().setQuery(
                boolQuery().should(termQuery("field1", "value1"))
                    .should(boolQuery().should(termQuery("field1", "value1")).should(termQuery("field1", "value2")).minimumShouldMatch(3))
                    .minimumShouldMatch(1)
            )
        );

        BoolQueryBuilder boolQuery = boolQuery().must(termQuery("field1", "value1"))
            .should(boolQuery().should(termQuery("field1", "value1")).should(termQuery("field1", "value2")).minimumShouldMatch(1))
            // Only one should clause is defined, returns no docs.
            .minimumShouldMatch(2);
        assertHitCount(prepareSearch().setQuery(boolQuery), 0L);

        boolQuery = boolQuery().must(termQuery("field1", "value1"))
            .must(boolQuery().should(termQuery("field1", "value1")).should(termQuery("field1", "value2")).minimumShouldMatch(3));
        assertHitCount(prepareSearch().setQuery(boolQuery), 0L);
    }

    public void testFuzzyQueryString() {
        createIndex("test");
        prepareIndex("test").setId("1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).get();
        prepareIndex("test").setId("2").setSource("str", "shay", "date", "2012-02-05", "num", 20).get();
        refresh();

        assertNoFailuresAndResponse(prepareSearch().setQuery(queryStringQuery("str:kimcy~1")), response -> {
            assertHitCount(response, 1L);
            assertFirstHit(response, hasId("1"));
        });
    }

    @TestIssueLogging(
        value = "org.elasticsearch.search.query.SearchQueryIT:DEBUG",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/43144"
    )
    public void testQuotedQueryStringWithBoost() throws InterruptedException {
        float boost = 10.0f;
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1)));

        indexRandom(
            true,
            false,
            prepareIndex("test").setId("1").setSource("important", "phrase match", "less_important", "nothing important"),
            prepareIndex("test").setId("2").setSource("important", "nothing important", "less_important", "phrase match")
        );

        assertResponse(
            prepareSearch().setQuery(queryStringQuery("\"phrase match\"").field("important", boost).field("less_important")),
            response -> {
                assertHitCount(response, 2L);
                assertFirstHit(response, hasId("1"));
                assertSecondHit(response, hasId("2"));
                assertThat((double) response.getHits().getAt(0).getScore(), closeTo(boost * response.getHits().getAt(1).getScore(), .1));
            }
        );
    }

    public void testSpecialRangeSyntaxInQueryString() {
        createIndex("test");
        prepareIndex("test").setId("1").setSource("str", "kimchy", "date", "2012-02-01", "num", 12).get();
        prepareIndex("test").setId("2").setSource("str", "shay", "date", "2012-02-05", "num", 20).get();
        refresh();

        assertResponses(response -> {
            assertHitCount(response, 1L);
            assertFirstHit(response, hasId("2"));
        }, prepareSearch().setQuery(queryStringQuery("num:>19")), prepareSearch().setQuery(queryStringQuery("num:>=20")));

        assertHitCount(prepareSearch().setQuery(queryStringQuery("num:>20")), 0L);
        assertHitCount(2L, prepareSearch().setQuery(queryStringQuery("num:>11")), prepareSearch().setQuery(queryStringQuery("num:<=20")));
        assertHitCount(
            1L,
            prepareSearch().setQuery(queryStringQuery("num:<20")),
            prepareSearch().setQuery(queryStringQuery("+num:>11 +num:<20"))
        );
    }

    public void testEmptytermsQuery() throws Exception {
        assertAcked(prepareCreate("test").setMapping("term", "type=text"));

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("term", "1"),
            prepareIndex("test").setId("2").setSource("term", "2"),
            prepareIndex("test").setId("3").setSource("term", "3"),
            prepareIndex("test").setId("4").setSource("term", "4")
        );
        assertHitCount(
            0L,
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("term", new String[0]))),
            prepareSearch("test").setQuery(idsQuery())
        );
    }

    public void testTermsQuery() throws Exception {
        assertAcked(prepareCreate("test").setMapping("str", "type=text", "lng", "type=long", "dbl", "type=double"));

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("str", "1", "lng", 1L, "dbl", 1.0d),
            prepareIndex("test").setId("2").setSource("str", "2", "lng", 2L, "dbl", 2.0d),
            prepareIndex("test").setId("3").setSource("str", "3", "lng", 3L, "dbl", 3.0d),
            prepareIndex("test").setId("4").setSource("str", "4", "lng", 4L, "dbl", 4.0d)
        );
        assertSearchHitsWithoutFailures(prepareSearch("test").setQuery(constantScoreQuery(termsQuery("str", "1", "4"))), "1", "4");
        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("lng", new long[] { 2, 3 }))),
            "2",
            "3"
        );
        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("dbl", new double[] { 2, 3 }))),
            "2",
            "3"
        );
        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("lng", new int[] { 1, 3 }))),
            "1",
            "3"
        );
        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("dbl", new float[] { 2, 4 }))),
            "2",
            "4"
        );
        // test partial matching
        assertSearchHitsWithoutFailures(prepareSearch("test").setQuery(constantScoreQuery(termsQuery("str", "2", "5"))), "2");
        assertSearchHitsWithoutFailures(prepareSearch("test").setQuery(constantScoreQuery(termsQuery("dbl", new double[] { 2, 5 }))), "2");
        assertSearchHitsWithoutFailures(prepareSearch("test").setQuery(constantScoreQuery(termsQuery("lng", new long[] { 2, 5 }))), "2");
        // test valid type, but no matching terms
        assertHitCount(
            0L,
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("str", "5", "6"))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("dbl", new double[] { 5, 6 }))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("lng", new long[] { 5, 6 })))
        );
    }

    public void testTermsLookupFilter() throws Exception {
        assertAcked(prepareCreate("lookup").setMapping("terms", "type=text", "other", "type=text"));
        assertAcked(
            prepareCreate("lookup2").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("arr")
                    .startObject("properties")
                    .startObject("term")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        assertAcked(prepareCreate("lookup3").setMapping("_source", "enabled=false", "terms", "type=text"));
        assertAcked(prepareCreate("test").setMapping("term", "type=text"));

        indexRandom(
            true,
            prepareIndex("lookup").setId("1").setSource("terms", new String[] { "1", "3" }),
            prepareIndex("lookup").setId("2").setSource("terms", new String[] { "2" }),
            prepareIndex("lookup").setId("3").setSource("terms", new String[] { "2", "4" }),
            prepareIndex("lookup").setId("4").setSource("other", "value"),
            prepareIndex("lookup2").setId("1")
                .setSource(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("arr")
                        .startObject()
                        .field("term", "1")
                        .endObject()
                        .startObject()
                        .field("term", "3")
                        .endObject()
                        .endArray()
                        .endObject()
                ),
            prepareIndex("lookup2").setId("2")
                .setSource(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("arr")
                        .startObject()
                        .field("term", "2")
                        .endObject()
                        .endArray()
                        .endObject()
                ),
            prepareIndex("lookup2").setId("3")
                .setSource(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("arr")
                        .startObject()
                        .field("term", "2")
                        .endObject()
                        .startObject()
                        .field("term", "4")
                        .endObject()
                        .endArray()
                        .endObject()
                ),
            prepareIndex("lookup3").setId("1").setSource("terms", new String[] { "1", "3" }),
            prepareIndex("test").setId("1").setSource("term", "1"),
            prepareIndex("test").setId("2").setSource("term", "2"),
            prepareIndex("test").setId("3").setSource("term", "3"),
            prepareIndex("test").setId("4").setSource("term", "4")
        );
        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(termsLookupQuery("term", new TermsLookup("lookup", "1", "terms"))),
            "1",
            "3"
        );

        // same as above, just on the _id...
        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(termsLookupQuery("_id", new TermsLookup("lookup", "1", "terms"))),
            "1",
            "3"
        );

        // another search with same parameters...
        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(termsLookupQuery("term", new TermsLookup("lookup", "1", "terms"))),
            "1",
            "3"
        );

        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(termsLookupQuery("term", new TermsLookup("lookup", "2", "terms"))),
            "2"
        );

        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(termsLookupQuery("term", new TermsLookup("lookup", "3", "terms"))),
            "2",
            "4"
        );

        assertHitCount(prepareSearch("test").setQuery(termsLookupQuery("term", new TermsLookup("lookup", "4", "terms"))), 0L);

        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(termsLookupQuery("term", new TermsLookup("lookup2", "1", "arr.term"))),
            "1",
            "3"
        );

        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(termsLookupQuery("term", new TermsLookup("lookup2", "2", "arr.term"))),
            "2"
        );

        assertSearchHitsWithoutFailures(
            prepareSearch("test").setQuery(termsLookupQuery("term", new TermsLookup("lookup2", "3", "arr.term"))),
            "2",
            "4"
        );

        assertHitCount(
            0L,
            prepareSearch("test").setQuery(termsLookupQuery("not_exists", new TermsLookup("lookup2", "3", "arr.term"))),
            // index "lookup" id "missing" document does not exist: ignore the lookup terms
            prepareSearch("test").setQuery(termsLookupQuery("term", new TermsLookup("lookup", "missing", "terms"))),
            // index "lookup3" has the source disabled: ignore the lookup terms
            prepareSearch("test").setQuery(termsLookupQuery("term", new TermsLookup("lookup3", "1", "terms")))
        );
    }

    public void testBasicQueryById() throws Exception {
        assertAcked(prepareCreate("test"));

        prepareIndex("test").setId("1").setSource("field1", "value1").get();
        prepareIndex("test").setId("2").setSource("field1", "value2").get();
        prepareIndex("test").setId("3").setSource("field1", "value3").get();
        refresh();

        assertResponse(prepareSearch().setQuery(idsQuery().addIds("1", "2")), response -> {
            assertHitCount(response, 2L);
            assertThat(response.getHits().getHits().length, equalTo(2));
        });
        assertResponse(prepareSearch().setQuery(idsQuery().addIds("1")), response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getHits().length, equalTo(1));
        });
        assertResponse(prepareSearch().setQuery(idsQuery().addIds("1", "2")), response -> {
            assertHitCount(response, 2L);
            assertThat(response.getHits().getHits().length, equalTo(2));
        });
        assertResponse(prepareSearch().setQuery(idsQuery().addIds("1")), response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getHits().length, equalTo(1));
        });
        assertResponse(prepareSearch().setQuery(idsQuery().addIds("1", "2", "3", "4")), response -> {
            assertHitCount(response, 3L);
            assertThat(response.getHits().getHits().length, equalTo(3));
        });
    }

    public void testNumericTermsAndRanges() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                "num_byte",
                "type=byte",
                "num_short",
                "type=short",
                "num_integer",
                "type=integer",
                "num_long",
                "type=long",
                "num_float",
                "type=float",
                "num_double",
                "type=double"
            )
        );

        prepareIndex("test").setId("1")
            .setSource("num_byte", 1, "num_short", 1, "num_integer", 1, "num_long", 1, "num_float", 1, "num_double", 1)
            .get();

        prepareIndex("test").setId("2")
            .setSource("num_byte", 2, "num_short", 2, "num_integer", 2, "num_long", 2, "num_float", 2, "num_double", 2)
            .get();

        prepareIndex("test").setId("17")
            .setSource("num_byte", 17, "num_short", 17, "num_integer", 17, "num_long", 17, "num_float", 17, "num_double", 17)
            .get();
        refresh();

        assertResponses(response -> {
            assertHitCount(response, 1L);
            assertFirstHit(response, hasId("1"));
        },
            prepareSearch("test").setQuery(termQuery("num_byte", 1)),
            prepareSearch("test").setQuery(termQuery("num_short", 1)),
            prepareSearch("test").setQuery(termQuery("num_integer", 1)),
            prepareSearch("test").setQuery(termQuery("num_long", 1)),
            prepareSearch("test").setQuery(termQuery("num_float", 1)),
            prepareSearch("test").setQuery(termQuery("num_double", 1)),
            prepareSearch("test").setQuery(termsQuery("num_byte", new int[] { 1 })),
            prepareSearch("test").setQuery(termsQuery("num_short", new int[] { 1 })),
            prepareSearch("test").setQuery(termsQuery("num_integer", new int[] { 1 })),
            prepareSearch("test").setQuery(termsQuery("num_long", new int[] { 1 })),
            prepareSearch("test").setQuery(termsQuery("num_float", new double[] { 1 })),
            prepareSearch("test").setQuery(termsQuery("num_double", new double[] { 1 })),
            prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_byte", 1))),
            prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_short", 1))),
            prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_integer", 1))),
            prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_long", 1))),
            prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_float", 1))),
            prepareSearch("test").setQuery(constantScoreQuery(termQuery("num_double", 1))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_byte", new int[] { 1 }))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_short", new int[] { 1 }))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_integer", new int[] { 1 }))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_long", new int[] { 1 }))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_float", new int[] { 1 }))),
            prepareSearch("test").setQuery(constantScoreQuery(termsQuery("num_double", new int[] { 1 })))
        );
    }

    public void testNumericRangeFilter_2826() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                "num_byte",
                "type=byte",
                "num_short",
                "type=short",
                "num_integer",
                "type=integer",
                "num_long",
                "type=long",
                "num_float",
                "type=float",
                "num_double",
                "type=double"
            )
        );

        prepareIndex("test").setId("1").setSource("field1", "test1", "num_long", 1).get();
        prepareIndex("test").setId("2").setSource("field1", "test1", "num_long", 2).get();
        prepareIndex("test").setId("3").setSource("field1", "test2", "num_long", 3).get();
        prepareIndex("test").setId("4").setSource("field1", "test2", "num_long", 4).get();
        refresh();

        assertHitCount(
            4L,
            prepareSearch("test").setPostFilter(
                boolQuery().should(rangeQuery("num_long").from(1).to(2)).should(rangeQuery("num_long").from(3).to(4))
            ),
            // This made 2826 fail! (only with bit based filters)
            prepareSearch("test").setPostFilter(
                boolQuery().should(rangeQuery("num_long").from(1).to(2)).should(rangeQuery("num_long").from(3).to(4))
            )
        );

        // This made #2979 fail!
        assertHitCount(
            prepareSearch("test").setPostFilter(
                boolQuery().must(termQuery("field1", "test1"))
                    .should(rangeQuery("num_long").from(1).to(2))
                    .should(rangeQuery("num_long").from(3).to(4))
            ),
            2L
        );
    }

    // see #2926
    public void testMustNot() throws InterruptedException {
        assertAcked(
            prepareCreate("test")
                // issue manifested only with shards>=2
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, between(2, DEFAULT_MAX_NUM_SHARDS)))
        );

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("description", "foo other anything bar"),
            prepareIndex("test").setId("2").setSource("description", "foo other anything"),
            prepareIndex("test").setId("3").setSource("description", "foo other"),
            prepareIndex("test").setId("4").setSource("description", "foo")
        );

        assertHitCount(prepareSearch("test").setQuery(matchAllQuery()).setSearchType(SearchType.DFS_QUERY_THEN_FETCH), 4L);
        assertHitCount(
            prepareSearch("test").setQuery(boolQuery().mustNot(matchQuery("description", "anything")))
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH),
            2L
        );
    }

    public void testIntervals() throws InterruptedException {
        createIndex("test");

        indexRandom(true, prepareIndex("test").setId("1").setSource("description", "it's cold outside, there's no kind of atmosphere"));

        String json = """
            {
              "intervals": {
                "description": {
                  "all_of": {
                    "ordered": "true",
                    "intervals": [
                      {
                        "any_of": {
                          "intervals": [ { "match": { "query": "cold" } }, { "match": { "query": "outside" } } ]
                        }
                      },
                      {
                        "match": {
                          "query": "atmosphere"
                        }
                      }
                    ],
                    "max_gaps": 30
                  }
                }
              }
            }""";
        assertHitCount(prepareSearch("test").setQuery(wrapperQuery(json)), 1L);
    }

    // see #2994
    public void testSimpleSpan() throws IOException, ExecutionException, InterruptedException {
        createIndex("test");

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("description", "foo other anything bar"),
            prepareIndex("test").setId("2").setSource("description", "foo other anything"),
            prepareIndex("test").setId("3").setSource("description", "foo other"),
            prepareIndex("test").setId("4").setSource("description", "foo")
        );

        assertHitCount(prepareSearch("test").setQuery(spanOrQuery(spanTermQuery("description", "bar"))), 1L);
        assertHitCount(
            prepareSearch("test").setQuery(
                spanNearQuery(spanTermQuery("description", "foo"), 3).addClause(spanTermQuery("description", "other"))
            ),
            3L
        );
    }

    public void testSpanMultiTermQuery() throws IOException {
        createIndex("test");

        prepareIndex("test").setId("1").setSource("description", "foo other anything bar", "count", 1).get();
        prepareIndex("test").setId("2").setSource("description", "foo other anything", "count", 2).get();
        prepareIndex("test").setId("3").setSource("description", "foo other", "count", 3).get();
        prepareIndex("test").setId("4").setSource("description", "fop", "count", 4).get();
        refresh();

        assertHitCount(
            4,
            prepareSearch("test").setQuery(spanOrQuery(spanMultiTermQueryBuilder(fuzzyQuery("description", "fop")))),
            prepareSearch("test").setQuery(spanOrQuery(spanMultiTermQueryBuilder(prefixQuery("description", "fo"))))
        );
        assertHitCount(
            3,
            prepareSearch("test").setQuery(spanOrQuery(spanMultiTermQueryBuilder(wildcardQuery("description", "oth*")))),
            prepareSearch("test").setQuery(
                spanOrQuery(spanMultiTermQueryBuilder(QueryBuilders.rangeQuery("description").from("ffa").to("foo")))
            ),
            prepareSearch("test").setQuery(spanOrQuery(spanMultiTermQueryBuilder(regexpQuery("description", "fo{2}"))))
        );
    }

    public void testSpanNot() throws IOException, ExecutionException, InterruptedException {
        createIndex("test");

        prepareIndex("test").setId("1").setSource("description", "the quick brown fox jumped over the lazy dog").get();
        prepareIndex("test").setId("2").setSource("description", "the quick black fox leaped over the sleeping dog").get();
        refresh();

        assertHitCount(
            1L,
            prepareSearch("test").setQuery(
                spanNotQuery(
                    spanNearQuery(QueryBuilders.spanTermQuery("description", "quick"), 1).addClause(
                        QueryBuilders.spanTermQuery("description", "fox")
                    ),
                    spanTermQuery("description", "brown")
                )
            ),
            prepareSearch("test").setQuery(
                spanNotQuery(
                    spanNearQuery(QueryBuilders.spanTermQuery("description", "quick"), 1).addClause(
                        QueryBuilders.spanTermQuery("description", "fox")
                    ),
                    spanTermQuery("description", "sleeping")
                ).dist(5)
            ),
            prepareSearch("test").setQuery(
                spanNotQuery(
                    spanNearQuery(QueryBuilders.spanTermQuery("description", "quick"), 1).addClause(
                        QueryBuilders.spanTermQuery("description", "fox")
                    ),
                    spanTermQuery("description", "jumped")
                ).pre(1).post(1)
            )
        );
    }

    public void testSimpleDFSQuery() throws IOException {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
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
                    .endObject()
            )
        );

        prepareIndex("test").setId("1")
            .setRouting("Y")
            .setSource("online", false, "bs", "Y", "ts", System.currentTimeMillis() - 100, "type", "s")
            .get();
        prepareIndex("test").setId("2")
            .setRouting("X")
            .setSource("online", true, "bs", "X", "ts", System.currentTimeMillis() - 10000000, "type", "s")
            .get();
        prepareIndex("test").setId("3")
            .setRouting(randomAlphaOfLength(2))
            .setSource("online", false, "ts", System.currentTimeMillis() - 100, "type", "bs")
            .get();
        prepareIndex("test").setId("4")
            .setRouting(randomAlphaOfLength(2))
            .setSource("online", true, "ts", System.currentTimeMillis() - 123123, "type", "bs")
            .get();
        refresh();

        assertNoFailures(
            prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(
                    boolQuery().must(termQuery("online", true))
                        .must(
                            boolQuery().should(
                                boolQuery().must(rangeQuery("ts").lt(System.currentTimeMillis() - (15 * 1000)))
                                    .must(termQuery("type", "bs"))
                            )
                                .should(
                                    boolQuery().must(rangeQuery("ts").lt(System.currentTimeMillis() - (15 * 1000)))
                                        .must(termQuery("type", "s"))
                                )
                        )
                )
                .setVersion(true)
                .setFrom(0)
                .setSize(100)
                .setExplain(true)
        );
    }

    public void testMultiFieldQueryString() {
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(
            1,
            prepareSearch("test").setQuery(queryStringQuery("value1").field("field1").field("field2")),
            prepareSearch("test").setQuery(queryStringQuery("field\\*:value1")),
            prepareSearch("test").setQuery(queryStringQuery("value*").field("field1").field("field2")),
            prepareSearch("test").setQuery(queryStringQuery("field\\*:value*")),
            prepareSearch("test").setQuery(queryStringQuery("v?lue*").field("field1").field("field2")),
            prepareSearch("test").setQuery(queryStringQuery("field\\*:v?lue*")),
            prepareSearch("test").setQuery(queryStringQuery("value~").field("field1").field("field2")),
            prepareSearch("test").setQuery(queryStringQuery("field\\*:value~")),
            prepareSearch("test").setQuery(queryStringQuery("/value[01]/").field("field1").field("field2")),
            prepareSearch("test").setQuery(queryStringQuery("field\\*:/value[01]/"))
        );
    }

    // see #3797
    public void testMultiMatchLenientIssue3797() {
        createIndex("test");

        prepareIndex("test").setId("1").setSource("field1", 123, "field2", "value2").get();
        refresh();

        assertHitCount(
            1L,
            prepareSearch("test").setQuery(multiMatchQuery("value2", "field2").field("field1", 2).lenient(true)),
            prepareSearch("test").setQuery(multiMatchQuery("value2", "field2").field("field1", 2).lenient(true)),
            prepareSearch("test").setQuery(multiMatchQuery("value2").field("field2", 2).lenient(true))
        );
    }

    public void testMinScore() throws ExecutionException, InterruptedException {
        createIndex("test");

        prepareIndex("test").setId("1").setSource("score", 1.5).get();
        prepareIndex("test").setId("2").setSource("score", 1.0).get();
        prepareIndex("test").setId("3").setSource("score", 2.0).get();
        prepareIndex("test").setId("4").setSource("score", 0.5).get();
        refresh();

        assertResponse(
            prepareSearch("test").setQuery(
                functionScoreQuery(ScoreFunctionBuilders.fieldValueFactorFunction("score").missing(1.0)).setMinScore(1.5f)
            ),
            response -> {
                assertHitCount(response, 2);
                assertFirstHit(response, hasId("3"));
                assertSecondHit(response, hasId("1"));
            }
        );
    }

    public void testQueryStringWithSlopAndFields() {
        assertAcked(prepareCreate("test"));

        prepareIndex("test").setId("1").setSource("desc", "one two three", "type", "customer").get();
        prepareIndex("test").setId("2").setSource("desc", "one two three", "type", "product").get();
        refresh();

        assertHitCount(prepareSearch("test").setQuery(QueryBuilders.queryStringQuery("\"one two\"").defaultField("desc")), 2);
        assertHitCount(
            1,
            prepareSearch("test").setPostFilter(QueryBuilders.termQuery("type", "customer"))
                .setQuery(QueryBuilders.queryStringQuery("\"one two\"").field("desc")),
            prepareSearch("test").setPostFilter(QueryBuilders.termQuery("type", "product"))
                .setQuery(QueryBuilders.queryStringQuery("\"one three\"~5").field("desc")),
            prepareSearch("test").setPostFilter(QueryBuilders.termQuery("type", "customer"))
                .setQuery(QueryBuilders.queryStringQuery("\"one two\"").defaultField("desc")),
            prepareSearch("test").setPostFilter(QueryBuilders.termQuery("type", "customer"))
                .setQuery(QueryBuilders.queryStringQuery("\"one two\"").defaultField("desc"))
        );
    }

    public void testDateProvidedAsNumber() throws InterruptedException {
        createIndex("test");
        assertAcked(indicesAdmin().preparePutMapping("test").setSource("field", "type=date,format=epoch_millis").get());
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field", 1000000000001L),
            prepareIndex("test").setId("2").setSource("field", 1000000000000L),
            prepareIndex("test").setId("3").setSource("field", 999999999999L),
            prepareIndex("test").setId("4").setSource("field", 1000000000002L),
            prepareIndex("test").setId("5").setSource("field", 1000000000003L),
            prepareIndex("test").setId("6").setSource("field", 999999999999L)
        );

        assertHitCount(prepareSearch("test").setSize(0).setQuery(rangeQuery("field").gte(1000000000000L)), 4);
        assertHitCount(prepareSearch("test").setSize(0).setQuery(rangeQuery("field").gte(999999999999L)), 6);
    }

    public void testRangeQueryWithTimeZone() throws Exception {
        assertAcked(prepareCreate("test").setMapping("date", "type=date", "num", "type=integer"));

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("date", "2014-01-01", "num", 1),
            prepareIndex("test").setId("2").setSource("date", "2013-12-31T23:00:00", "num", 2),
            prepareIndex("test").setId("3").setSource("date", "2014-01-01T01:00:00", "num", 3),
            // Now in UTC+1
            prepareIndex("test").setId("4")
                .setSource("date", Instant.now().atZone(ZoneOffset.ofHours(1)).toInstant().toEpochMilli(), "num", 4)
        );

        assertResponses(response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getAt(0).getId(), is("1"));
        },
            prepareSearch("test").setQuery(QueryBuilders.rangeQuery("date").from("2014-01-01T00:00:00").to("2014-01-01T00:59:00")),
            // We explicitly define a time zone in the from/to dates so whatever the time zone is, it won't be used
            prepareSearch("test").setQuery(
                QueryBuilders.rangeQuery("date").from("2014-01-01T00:00:00Z").to("2014-01-01T00:59:00Z").timeZone("+10:00")
            ),
            // We define a time zone to be applied to the filter and from/to have no time zone
            prepareSearch("test").setQuery(
                QueryBuilders.rangeQuery("date").from("2014-01-01T03:00:00").to("2014-01-01T03:59:00").timeZone("+03:00")
            )
        );
        assertResponses(response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getAt(0).getId(), is("2"));
        },
            prepareSearch("test").setQuery(QueryBuilders.rangeQuery("date").from("2013-12-31T23:00:00").to("2013-12-31T23:59:00")),
            prepareSearch("test").setQuery(
                QueryBuilders.rangeQuery("date").from("2013-12-31T23:00:00Z").to("2013-12-31T23:59:00Z").timeZone("+10:00")
            ),
            prepareSearch("test").setQuery(
                QueryBuilders.rangeQuery("date").from("2014-01-01T02:00:00").to("2014-01-01T02:59:00").timeZone("+03:00")
            )
        );
        assertResponses(response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getAt(0).getId(), is("3"));
        },
            prepareSearch("test").setQuery(QueryBuilders.rangeQuery("date").from("2014-01-01T01:00:00").to("2014-01-01T01:59:00")),
            prepareSearch("test").setQuery(
                QueryBuilders.rangeQuery("date").from("2014-01-01T01:00:00Z").to("2014-01-01T01:59:00Z").timeZone("+10:00")
            )
        );
        assertResponses(response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getAt(0).getId(), is("3"));
        },
            prepareSearch("test").setQuery(
                QueryBuilders.rangeQuery("date").from("2014-01-01").to("2014-01-01T00:59:00").timeZone("-01:00")
            ),
            prepareSearch("test").setQuery(
                QueryBuilders.rangeQuery("date").from("2014-01-01T04:00:00").to("2014-01-01T04:59:00").timeZone("+03:00")
            )
        );
        assertResponse(prepareSearch("test").setQuery(QueryBuilders.rangeQuery("date").from("now/d-1d").timeZone("+01:00")), response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getAt(0).getId(), is("4"));
        });
    }

    /**
     * Test range with a custom locale, e.g. "de" in this case. Documents here mention the day of week
     * as "Mi" for "Mittwoch (Wednesday" and "Do" for "Donnerstag (Thursday)" and the month in the query
     * as "Dez" for "Dezember (December)".
     */
    public void testRangeQueryWithLocaleMapping() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("date_field")
                    .field("type", "date")
                    .field("format", "E, d MMM yyyy HH:mm:ss Z")
                    .field("locale", "fr")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("date_field", "mer., 6 déc. 2000 02:55:00 -0800"),
            prepareIndex("test").setId("2").setSource("date_field", "jeu., 7 déc. 2000 02:55:00 -0800")
        );

        assertHitCount(
            prepareSearch("test").setQuery(
                QueryBuilders.rangeQuery("date_field").gte("mar., 5 déc. 2000 02:55:00 -0800").lte("jeu., 7 déc. 2000 00:00:00 -0800")
            ),
            1L
        );
        assertHitCount(
            prepareSearch("test").setQuery(
                QueryBuilders.rangeQuery("date_field").gte("mar., 5 déc. 2000 02:55:00 -0800").lte("ven., 8 déc. 2000 00:00:00 -0800")
            ),
            2L
        );
    }

    public void testSearchEmptyDoc() {
        prepareIndex("test").setId("1").setSource("{}", XContentType.JSON).get();

        refresh();
        assertHitCount(prepareSearch().setQuery(matchAllQuery()), 1L);
    }

    public void testMatchPhrasePrefixQuery() throws ExecutionException, InterruptedException {
        createIndex("test1");
        indexRandom(
            true,
            prepareIndex("test1").setId("1").setSource("field", "Johnnie Walker Black Label"),
            prepareIndex("test1").setId("2").setSource("field", "trying out Elasticsearch")
        );

        assertSearchHitsWithoutFailures(prepareSearch().setQuery(matchPhrasePrefixQuery("field", "Johnnie la").slop(between(2, 5))), "1");
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(matchPhrasePrefixQuery("field", "trying")), "2");
        assertSearchHitsWithoutFailures(prepareSearch().setQuery(matchPhrasePrefixQuery("field", "try")), "2");
    }

    public void testQueryStringParserCache() throws Exception {
        createIndex("test");
        indexRandom(true, false, prepareIndex("test").setId("1").setSource("nameTokens", "xyz"));
        final float[] first = new float[1];
        assertResponse(
            prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.queryStringQuery("xyz").boost(100)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                first[0] = response.getHits().getAt(0).getScore();
            }
        );
        for (int i = 0; i < 100; i++) {
            final int finalI = i;
            assertResponse(
                prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.queryStringQuery("xyz").boost(100)),
                response -> {
                    assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                    assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                    float actual = response.getHits().getAt(0).getScore();
                    assertThat(finalI + " expected: " + first[0] + " actual: " + actual, Float.compare(first[0], actual), equalTo(0));
                }
            );
        }
    }

    public void testRangeQueryRangeFields_24744() throws Exception {
        assertAcked(prepareCreate("test").setMapping("int_range", "type=integer_range"));

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().startObject("int_range").field("gte", 10).field("lte", 20).endObject().endObject())
            .get();
        refresh();

        RangeQueryBuilder range = new RangeQueryBuilder("int_range").relation("intersects").from(Integer.MIN_VALUE).to(Integer.MAX_VALUE);
        assertHitCount(prepareSearch("test").setQuery(range), 1L);
    }

    public void testNestedQueryWithFieldAlias() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("section")
            .field("type", "nested")
            .startObject("properties")
            .startObject("distance")
            .field("type", "long")
            .endObject()
            .startObject("route_length_miles")
            .field("type", "alias")
            .field("path", "section.distance")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("index").setMapping(mapping));

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("section")
            .field("distance", 42)
            .endObject()
            .endObject();

        index("index", "1", source);
        refresh();

        QueryBuilder nestedQuery = QueryBuilders.nestedQuery(
            "section",
            QueryBuilders.termQuery("section.route_length_miles", 42),
            ScoreMode.Max
        );
        assertHitCount(prepareSearch("index").setQuery(nestedQuery), 1);
    }

    public void testFieldAliasesForMetaFields() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("id-alias")
            .field("type", "alias")
            .field("path", "_id")
            .endObject()
            .startObject("routing-alias")
            .field("type", "alias")
            .field("path", "_routing")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").setMapping(mapping));

        IndexRequestBuilder indexRequest = prepareIndex("test").setId("1").setRouting("custom").setSource("field", "value");
        indexRandom(true, false, indexRequest);
        updateClusterSettings(Settings.builder().put(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey(), true));
        try {
            assertResponse(prepareSearch().setQuery(termQuery("routing-alias", "custom")).addDocValueField("id-alias"), response -> {
                assertHitCount(response, 1L);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(2, hit.getFields().size());
                assertTrue(hit.getFields().containsKey("id-alias"));

                DocumentField field = hit.getFields().get("id-alias");
                assertThat(field.getValue().toString(), equalTo("1"));
            });
        } finally {
            // unset cluster setting
            updateClusterSettings(Settings.builder().putNull(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()));
        }

    }

    /**
     * Test that wildcard queries on keyword fields get normalized
     */
    public void testWildcardQueryNormalizationOnKeywordField() {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put("index.analysis.normalizer.lowercase_normalizer.type", "custom")
                    .putList("index.analysis.normalizer.lowercase_normalizer.filter", "lowercase")
                    .build()
            ).setMapping("field1", "type=keyword,normalizer=lowercase_normalizer")
        );
        prepareIndex("test").setId("1").setSource("field1", "Bbb Aaa").get();
        refresh();

        {
            assertHitCount(
                1L,
                prepareSearch().setQuery(wildcardQuery("field1", "Bb*")),
                prepareSearch().setQuery(wildcardQuery("field1", "bb*"))
            );
        }
    }

    /**
     * Test that wildcard queries on text fields don't get normalized
     */
    public void testWildcardQueryNormalizationOnTextField() {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put("index.analysis.analyzer.lowercase_analyzer.type", "custom")
                    .put("index.analysis.analyzer.lowercase_analyzer.tokenizer", "standard")
                    .putList("index.analysis.analyzer.lowercase_analyzer.filter", "lowercase")
                    .build()
            ).setMapping("field1", "type=text,analyzer=lowercase_analyzer")
        );
        prepareIndex("test").setId("1").setSource("field1", "Bbb Aaa").get();
        refresh();

        {
            WildcardQueryBuilder wildCardQuery = wildcardQuery("field1", "Bb*");
            assertHitCount(prepareSearch().setQuery(wildCardQuery), 0L);

            assertHitCount(
                1L,
                // the following works not because of normalization but because of the `case_insensitive` parameter
                prepareSearch().setQuery(wildcardQuery("field1", "Bb*").caseInsensitive(true)),
                prepareSearch().setQuery(wildcardQuery("field1", "bb*"))
            );
        }
    }

    /**
     * Reserved characters should be excluded when the normalization is applied for keyword fields.
     * See https://github.com/elastic/elasticsearch/issues/46300 for details.
     */
    public void testWildcardQueryNormalizationKeywordSpecialCharacters() {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put("index.analysis.char_filter.no_wildcard.type", "mock_pattern_replace")
                    .put("index.analysis.normalizer.no_wildcard.type", "custom")
                    .put("index.analysis.normalizer.no_wildcard.char_filter", "no_wildcard")
                    .build()
            ).setMapping("field", "type=keyword,normalizer=no_wildcard")
        );
        prepareIndex("test").setId("1").setSource("field", "label-1").get();
        refresh();

        assertHitCount(
            1L,
            prepareSearch().setQuery(wildcardQuery("field", "la*")),
            prepareSearch().setQuery(wildcardQuery("field", "la*el-?"))
        );
    }

    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {

        @Override
        public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
            return singletonMap("mock_pattern_replace", (indexSettings, env, name, settings) -> {
                class Factory implements NormalizingCharFilterFactory {

                    private final Pattern pattern = Regex.compile("[\\*\\?]", null);

                    @Override
                    public String name() {
                        return name;
                    }

                    @Override
                    public Reader create(Reader reader) {
                        return new PatternReplaceCharFilter(pattern, "", reader);
                    }
                }
                return new Factory();
            });
        }

        @Override
        public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
            return singletonMap(
                "keyword",
                (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(
                    name,
                    () -> new MockTokenizer(MockTokenizer.KEYWORD, false)
                )
            );
        }
    }

    /**
     * Test correct handling
     * {@link SpanBooleanQueryRewriteWithMaxClause#rewrite(IndexSearcher, MultiTermQuery)}.
     * That rewrite method is e.g.
     * set for fuzzy queries with "constant_score" rewrite nested inside a `span_multi` query and would cause NPEs due to an unset
     * {@link AttributeSource}.
     */
    public void testIssueFuzzyInsideSpanMulti() {
        createIndex("test");
        prepareIndex("test").setId("1").setSource("field", "foobarbaz").get();
        ensureGreen();
        refresh();

        BoolQueryBuilder query = boolQuery().filter(spanMultiTermQueryBuilder(fuzzyQuery("field", "foobarbiz").rewrite("constant_score")));
        assertHitCount(prepareSearch("test").setQuery(query), 1);
    }

    public void testFetchIdFieldQuery() {
        createIndex("test");
        int docCount = randomIntBetween(10, 50);
        for (int i = 0; i < docCount; i++) {
            prepareIndex("test").setSource("field", "foobarbaz").get();
        }
        ensureGreen();
        refresh();

        assertResponse(prepareSearch("test").addFetchField("_id").setSize(docCount), response -> {
            SearchHit[] hits = response.getHits().getHits();
            assertEquals(docCount, hits.length);
            for (SearchHit hit : hits) {
                assertNotNull(hit.getFields().get("_id").getValue());
            }
        });
    }
}
