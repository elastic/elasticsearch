/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.functionscore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.tests.util.English;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.rescore.QueryRescoreMode;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.lucene.search.function.CombineFunction.REPLACE;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFourthHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponses;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThirdHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasScore;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class QueryRescorerIT extends ESIntegTestCase {
    public void testEnforceWindowSize() {
        createIndex("test");
        // this
        int iters = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < iters; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("f", Integer.toString(i)).get();
        }
        refresh();

        int numShards = getNumShards("test").numPrimaries;
        for (int j = 0; j < iters; j++) {
            assertNoFailuresAndResponse(
                prepareSearch().setQuery(QueryBuilders.matchAllQuery())
                    .setRescorer(
                        new QueryRescorerBuilder(
                            functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.weightFactorFunction(100)).boostMode(
                                CombineFunction.REPLACE
                            ).queryName("hello world")
                        ).setQueryWeight(0.0f).setRescoreQueryWeight(1.0f),
                        1
                    )
                    .setSize(randomIntBetween(2, 10)),
                response -> {
                    assertFirstHit(response, hasScore(100.f));
                    int numDocsWith100AsAScore = 0;
                    for (int i = 0; i < response.getHits().getHits().length; i++) {
                        float score = response.getHits().getHits()[i].getScore();
                        if (score == 100f) {
                            numDocsWith100AsAScore += 1;
                        }
                    }
                    assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
                    // we cannot assert that they are equal since some shards might not have docs at all
                    assertThat(numDocsWith100AsAScore, lessThanOrEqualTo(numShards));
                }
            );
        }
    }

    public void testRescorePhrase() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field1")
                    .field("analyzer", "whitespace")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            ).setSettings(Settings.builder().put(indexSettings()).put("index.number_of_shards", 1))
        );

        prepareIndex("test").setId("1").setSource("field1", "the quick brown fox").get();
        prepareIndex("test").setId("2").setSource("field1", "the quick lazy huge brown fox jumps over the tree ").get();
        prepareIndex("test").setId("3")
            .setSource("field1", "quick huge brown", "field2", "the quick lazy huge brown fox jumps over the tree")
            .get();
        refresh();
        assertResponse(
            prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
                .setRescorer(
                    new QueryRescorerBuilder(matchPhraseQuery("field1", "quick brown").slop(2).boost(4.0f)).setRescoreQueryWeight(2),
                    5
                ),
            response -> {
                assertHitCount(response, 3);
                assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
                assertFirstHit(response, hasId("1"));
                assertSecondHit(response, hasId("3"));
                assertThirdHit(response, hasId("2"));
            }
        );
        assertResponses(response -> {
            assertHitCount(response, 3);
            assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
            assertFirstHit(response, hasId("1"));
            assertSecondHit(response, hasId("2"));
            assertThirdHit(response, hasId("3"));
        },
            prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
                .setRescorer(new QueryRescorerBuilder(matchPhraseQuery("field1", "the quick brown").slop(3)), 5),
            prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
                .setRescorer(new QueryRescorerBuilder(matchPhraseQuery("field1", "the quick brown")), 5)
        );
    }

    public void testMoreDocs() throws Exception {
        Builder builder = Settings.builder();

        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .field("analyzer", "whitespace")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("test").setMapping(mapping).setSettings(builder.put("index.number_of_shards", 1)));

        prepareIndex("test").setId("1").setSource("field1", "massachusetts avenue boston massachusetts").get();
        prepareIndex("test").setId("2").setSource("field1", "lexington avenue boston massachusetts").get();
        prepareIndex("test").setId("3").setSource("field1", "boston avenue lexington massachusetts").get();
        indicesAdmin().prepareRefresh("test").get();
        prepareIndex("test").setId("4").setSource("field1", "boston road lexington massachusetts").get();
        prepareIndex("test").setId("5").setSource("field1", "lexington street lexington massachusetts").get();
        prepareIndex("test").setId("6").setSource("field1", "massachusetts avenue lexington massachusetts").get();
        prepareIndex("test").setId("7").setSource("field1", "bosten street san franciso california").get();
        indicesAdmin().prepareRefresh("test").get();
        prepareIndex("test").setId("8").setSource("field1", "hollywood boulevard los angeles california").get();
        prepareIndex("test").setId("9").setSource("field1", "1st street boston massachussetts").get();
        prepareIndex("test").setId("10").setSource("field1", "1st street boston massachusetts").get();
        indicesAdmin().prepareRefresh("test").get();
        prepareIndex("test").setId("11").setSource("field1", "2st street boston massachusetts").get();
        prepareIndex("test").setId("12").setSource("field1", "3st street boston massachusetts").get();
        indicesAdmin().prepareRefresh("test").get();

        assertResponses(response -> {
            assertThat(response.getHits().getHits().length, equalTo(5));
            assertHitCount(response, 9);
            assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
            assertFirstHit(response, hasId("2"));
            assertSecondHit(response, hasId("6"));
            assertThirdHit(response, hasId("3"));
        },
            prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "lexington avenue massachusetts").operator(Operator.OR))
                .setFrom(0)
                .setSize(5)
                .setRescorer(
                    new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(0.6f)
                        .setRescoreQueryWeight(2.0f),
                    20
                ),
            prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "lexington avenue massachusetts").operator(Operator.OR))
                .setFrom(0)
                .setSize(5)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setRescorer(
                    new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(0.6f)
                        .setRescoreQueryWeight(2.0f),
                    20
                )
        );
        // Make sure non-zero from works:
        assertResponse(
            prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "lexington avenue massachusetts").operator(Operator.OR))
                .setFrom(2)
                .setSize(5)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setRescorer(
                    new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(0.6f)
                        .setRescoreQueryWeight(2.0f),
                    20
                ),
            response -> {
                assertThat(response.getHits().getHits().length, equalTo(5));
                assertHitCount(response, 9);
                assertThat(response.getHits().getMaxScore(), greaterThan(response.getHits().getHits()[0].getScore()));
                assertFirstHit(response, hasId("3"));
            }
        );
    }

    // Tests a rescore window smaller than number of hits:
    public void testSmallRescoreWindow() throws Exception {
        Builder builder = Settings.builder();

        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .field("analyzer", "whitespace")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("test").setMapping(mapping).setSettings(builder.put("index.number_of_shards", 1)));

        prepareIndex("test").setId("3").setSource("field1", "massachusetts").get();
        prepareIndex("test").setId("6").setSource("field1", "massachusetts avenue lexington massachusetts").get();
        indicesAdmin().prepareRefresh("test").get();
        prepareIndex("test").setId("1").setSource("field1", "lexington massachusetts avenue").get();
        prepareIndex("test").setId("2").setSource("field1", "lexington avenue boston massachusetts road").get();
        indicesAdmin().prepareRefresh("test").get();

        assertResponse(prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "massachusetts")).setFrom(0).setSize(5), response -> {
            assertThat(response.getHits().getHits().length, equalTo(4));
            assertHitCount(response, 4);
            assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
            assertFirstHit(response, hasId("3"));
            assertSecondHit(response, hasId("6"));
            assertThirdHit(response, hasId("1"));
            assertFourthHit(response, hasId("2"));
        });

        // Now, rescore only top 2 hits w/ proximity:
        assertResponse(
            prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "massachusetts"))
                .setFrom(0)
                .setSize(5)
                .setRescorer(
                    new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(0.6f)
                        .setRescoreQueryWeight(2.0f),
                    2
                ),
            response -> {
                // Only top 2 hits were re-ordered:
                assertThat(response.getHits().getHits().length, equalTo(4));
                assertHitCount(response, 4);
                assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
                assertFirstHit(response, hasId("6"));
                assertSecondHit(response, hasId("3"));
                assertThirdHit(response, hasId("1"));
                assertFourthHit(response, hasId("2"));
            }
        );

        // Now, rescore only top 3 hits w/ proximity:
        assertResponse(
            prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "massachusetts"))
                .setFrom(0)
                .setSize(5)
                .setRescorer(
                    new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(0.6f)
                        .setRescoreQueryWeight(2.0f),
                    3
                ),
            response -> {
                // Only top 3 hits were re-ordered:
                assertThat(response.getHits().getHits().length, equalTo(4));
                assertHitCount(response, 4);
                assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
                assertFirstHit(response, hasId("1"));
                assertSecondHit(response, hasId("6"));
                assertThirdHit(response, hasId("3"));
                assertFourthHit(response, hasId("2"));
            }
        );
    }

    // Tests a rescorer that penalizes the scores:
    public void testRescorerMadeScoresWorse() throws Exception {
        Builder builder = Settings.builder();

        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .field("analyzer", "whitespace")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertAcked(indicesAdmin().prepareCreate("test").setMapping(mapping).setSettings(builder.put("index.number_of_shards", 1)));

        prepareIndex("test").setId("3").setSource("field1", "massachusetts").get();
        prepareIndex("test").setId("6").setSource("field1", "massachusetts avenue lexington massachusetts").get();
        indicesAdmin().prepareRefresh("test").get();
        prepareIndex("test").setId("1").setSource("field1", "lexington massachusetts avenue").get();
        prepareIndex("test").setId("2").setSource("field1", "lexington avenue boston massachusetts road").get();
        indicesAdmin().prepareRefresh("test").get();

        assertResponse(
            prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "massachusetts").operator(Operator.OR)).setFrom(0).setSize(5),
            response -> {
                assertThat(response.getHits().getHits().length, equalTo(4));
                assertHitCount(response, 4);
                assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
                assertFirstHit(response, hasId("3"));
                assertSecondHit(response, hasId("6"));
                assertThirdHit(response, hasId("1"));
                assertFourthHit(response, hasId("2"));
            }
        );
        // Now, penalizing rescore (nothing matches the rescore query):
        assertResponse(
            prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "massachusetts").operator(Operator.OR))
                .setFrom(0)
                .setSize(5)
                .setRescorer(
                    new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(1.0f)
                        .setRescoreQueryWeight(-1f),
                    3
                ),
            response -> {
                // 6 and 1 got worse, and then the hit (2) outside the rescore window were sorted ahead:
                assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
                assertFirstHit(response, hasId("3"));
                assertSecondHit(response, hasId("2"));
                assertThirdHit(response, hasId("6"));
                assertFourthHit(response, hasId("1"));
            }
        );
    }

    // Comparator that sorts hits and rescored hits in the same way.
    // The rescore uses the docId as tie, while regular search uses the slot the hit is in as a tie if score
    // and shard id are equal during merging shard results.
    // This comparator uses a custom tie in case the scores are equal, so that both regular hits and rescored hits
    // are sorted equally. This is fine since tests only care about the fact the scores should be equal, not ordering.
    private static final Comparator<SearchHit> searchHitsComparator = new Comparator<SearchHit>() {
        @Override
        public int compare(SearchHit hit1, SearchHit hit2) {
            int cmp = Float.compare(hit2.getScore(), hit1.getScore());
            if (cmp == 0) {
                return hit1.getId().compareTo(hit2.getId());
            } else {
                return cmp;
            }
        }
    };

    private static void assertEquivalent(String query, SearchResponse plain, SearchResponse rescored) {
        assertNoFailures(plain);
        assertNoFailures(rescored);
        SearchHits leftHits = plain.getHits();
        SearchHits rightHits = rescored.getHits();
        assertThat(leftHits.getTotalHits().value(), equalTo(rightHits.getTotalHits().value()));
        assertThat(leftHits.getHits().length, equalTo(rightHits.getHits().length));
        SearchHit[] hits = leftHits.getHits();
        SearchHit[] rHits = rightHits.getHits();
        Arrays.sort(hits, searchHitsComparator);
        Arrays.sort(rHits, searchHitsComparator);
        for (int i = 0; i < hits.length; i++) {
            assertThat("query: " + query, hits[i].getScore(), equalTo(rHits[i].getScore()));
        }
        for (int i = 0; i < hits.length; i++) {
            if (hits[i].getScore() == hits[hits.length - 1].getScore()) {
                return; // we need to cut off here since this is the tail of the queue and we might not have fetched enough docs
            }
            assertThat("query: " + query, hits[i].getId(), equalTo(rHits[i].getId()));
        }
    }

    // forces QUERY_THEN_FETCH because of https://github.com/elastic/elasticsearch/issues/4829
    public void testEquivalence() throws Exception {
        // no dummy docs since merges can change scores while we run queries.
        int numDocs = indexRandomNumbers("whitespace", -1, false);

        final int iters = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < iters; i++) {
            int resultSize = numDocs;
            int rescoreWindow = between(1, 3) * resultSize;
            String intToEnglish = English.intToEnglish(between(0, numDocs - 1));
            String query = intToEnglish.split(" ")[0];

            assertResponse(
                prepareSearch().setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setPreference("test") // ensure we hit the same shards for tie-breaking
                    .setQuery(QueryBuilders.matchQuery("field1", query).operator(Operator.OR))
                    .setFrom(0)
                    .setSize(resultSize),
                plain -> {
                    assertResponses(
                        rescored -> assertEquivalent(query, plain, rescored),
                        prepareSearch().setSearchType(SearchType.QUERY_THEN_FETCH)
                            .setPreference("test") // ensure we hit the same shards for tie-breaking
                            .setQuery(QueryBuilders.matchQuery("field1", query).operator(Operator.OR))
                            .setFrom(0)
                            .setSize(resultSize)
                            .setRescorer(
                                new QueryRescorerBuilder(constantScoreQuery(matchPhraseQuery("field1", intToEnglish).slop(3)))
                                    .setQueryWeight(1.0f)
                                    // no weight - so we basically use the same score as the actual query
                                    .setRescoreQueryWeight(0.0f),
                                rescoreWindow
                            ),
                        prepareSearch().setSearchType(SearchType.QUERY_THEN_FETCH)
                            .setPreference("test") // ensure we hit the same shards for tie-breaking
                            .setQuery(QueryBuilders.matchQuery("field1", query).operator(Operator.OR))
                            .setFrom(0)
                            .setSize(resultSize)
                            .setRescorer(
                                new QueryRescorerBuilder(constantScoreQuery(matchPhraseQuery("field1", "not in the index").slop(3)))
                                    .setQueryWeight(1.0f)
                                    .setRescoreQueryWeight(1.0f),
                                rescoreWindow
                            )
                    );  // check equivalence
                }
            );
        }
    }

    public void testExplain() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field1")
                    .field("analyzer", "whitespace")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        prepareIndex("test").setId("1").setSource("field1", "the quick brown fox").get();
        prepareIndex("test").setId("2").setSource("field1", "the quick lazy huge brown fox jumps over the tree").get();
        prepareIndex("test").setId("3")
            .setSource("field1", "quick huge brown", "field2", "the quick lazy huge brown fox jumps over the tree")
            .get();
        refresh();

        {
            assertResponse(
                prepareSearch().setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
                    .setRescorer(
                        new QueryRescorerBuilder(matchPhraseQuery("field1", "the quick brown").slop(2).boost(4.0f)).setQueryWeight(0.5f)
                            .setRescoreQueryWeight(0.4f),
                        5
                    )
                    .setExplain(true),
                response -> {
                    assertHitCount(response, 3);
                    assertFirstHit(response, hasId("1"));
                    assertSecondHit(response, hasId("2"));
                    assertThirdHit(response, hasId("3"));

                    for (int i = 0; i < 3; i++) {
                        assertThat(response.getHits().getAt(i).getExplanation(), notNullValue());
                        assertThat(response.getHits().getAt(i).getExplanation().isMatch(), equalTo(true));
                        assertThat(response.getHits().getAt(i).getExplanation().getDetails().length, equalTo(2));
                        assertThat(response.getHits().getAt(i).getExplanation().getDetails()[0].isMatch(), equalTo(true));
                        if (i == 2) {
                            assertThat(response.getHits().getAt(i).getExplanation().getDetails()[1].getValue(), equalTo(0.5f));
                        } else {
                            assertThat(response.getHits().getAt(i).getExplanation().getDescription(), equalTo("sum of:"));
                            assertThat(
                                response.getHits().getAt(i).getExplanation().getDetails()[0].getDetails()[1].getValue(),
                                equalTo(0.5f)
                            );
                            assertThat(
                                response.getHits().getAt(i).getExplanation().getDetails()[1].getDetails()[1].getValue(),
                                equalTo(0.4f)
                            );
                        }
                    }
                }
            );
        }

        String[] scoreModes = new String[] { "max", "min", "avg", "total", "multiply", "" };
        String[] descriptionModes = new String[] { "max of:", "min of:", "avg of:", "sum of:", "product of:", "sum of:" };
        for (int innerMode = 0; innerMode < scoreModes.length; innerMode++) {
            QueryRescorerBuilder innerRescoreQuery = new QueryRescorerBuilder(matchQuery("field1", "the quick brown").boost(4.0f))
                .setQueryWeight(0.5f)
                .setRescoreQueryWeight(0.4f);

            if ("".equals(scoreModes[innerMode]) == false) {
                innerRescoreQuery.setScoreMode(QueryRescoreMode.fromString(scoreModes[innerMode]));
            }
            final int finalInnerMode = innerMode;
            assertResponse(
                prepareSearch().setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
                    .setRescorer(innerRescoreQuery, 5)
                    .setExplain(true),
                response -> {
                    assertHitCount(response, 3);
                    assertFirstHit(response, hasId("1"));
                    assertSecondHit(response, hasId("2"));
                    assertThirdHit(response, hasId("3"));

                    for (int j = 0; j < 3; j++) {
                        assertThat(
                            response.getHits().getAt(j).getExplanation().getDescription(),
                            equalTo(descriptionModes[finalInnerMode])
                        );
                    }
                }
            );
            for (int outerMode = 0; outerMode < scoreModes.length; outerMode++) {
                QueryRescorerBuilder outerRescoreQuery = new QueryRescorerBuilder(matchQuery("field1", "the quick brown").boost(4.0f))
                    .setQueryWeight(0.5f)
                    .setRescoreQueryWeight(0.4f);

                if ("".equals(scoreModes[outerMode]) == false) {
                    outerRescoreQuery.setScoreMode(QueryRescoreMode.fromString(scoreModes[outerMode]));
                }
                final int finalOuterMode = outerMode;
                assertResponse(
                    prepareSearch().setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
                        .addRescorer(innerRescoreQuery, 5)
                        .addRescorer(outerRescoreQuery.windowSize(10))
                        .setExplain(true),
                    response -> {
                        assertHitCount(response, 3);
                        assertFirstHit(response, hasId("1"));
                        assertSecondHit(response, hasId("2"));
                        assertThirdHit(response, hasId("3"));

                        for (int j = 0; j < 3; j++) {
                            Explanation explanation = response.getHits().getAt(j).getExplanation();
                            assertThat(explanation.getDescription(), equalTo(descriptionModes[finalOuterMode]));
                            assertThat(
                                explanation.getDetails()[0].getDetails()[0].getDescription(),
                                equalTo(descriptionModes[finalInnerMode])
                            );
                        }
                    }
                );
            }
        }
    }

    public void testScoring() throws Exception {
        int numDocs = indexRandomNumbers("keyword");

        String[] scoreModes = new String[] { "max", "min", "avg", "total", "multiply", "" };
        float primaryWeight = 1.1f;
        float secondaryWeight = 1.6f;

        for (String scoreMode : scoreModes) {
            for (int i = 0; i < numDocs - 4; i++) {
                String[] intToEnglish = new String[] {
                    English.intToEnglish(i),
                    English.intToEnglish(i + 1),
                    English.intToEnglish(i + 2),
                    English.intToEnglish(i + 3) };

                QueryBuilder query = boolQuery().should(
                    functionScoreQuery(termQuery("field1", intToEnglish[0]), weightFactorFunction(2.0f)).boostMode(REPLACE)
                )
                    .should(functionScoreQuery(termQuery("field1", intToEnglish[1]), weightFactorFunction(3.0f)).boostMode(REPLACE))
                    .should(functionScoreQuery(termQuery("field1", intToEnglish[2]), weightFactorFunction(5.0f)).boostMode(REPLACE))
                    .should(functionScoreQuery(termQuery("field1", intToEnglish[3]), weightFactorFunction(0.2f)).boostMode(REPLACE));
                QueryRescorerBuilder rescoreQuery = new QueryRescorerBuilder(
                    boolQuery().should(
                        functionScoreQuery(termQuery("field1", intToEnglish[0]), weightFactorFunction(5.0f)).boostMode(REPLACE)
                    )
                        .should(functionScoreQuery(termQuery("field1", intToEnglish[1]), weightFactorFunction(7.0f)).boostMode(REPLACE))
                        .should(functionScoreQuery(termQuery("field1", intToEnglish[3]), weightFactorFunction(0.0f)).boostMode(REPLACE))
                );
                rescoreQuery.setQueryWeight(primaryWeight).setRescoreQueryWeight(secondaryWeight);

                if ("".equals(scoreMode) == false) {
                    rescoreQuery.setScoreMode(QueryRescoreMode.fromString(scoreMode));
                }
                final int finalI = i;
                assertResponse(
                    prepareSearch().setPreference("test") // ensure we hit the same shards for tie-breaking
                        .setFrom(0)
                        .setSize(10)
                        .setQuery(query)
                        .setRescorer(rescoreQuery, 50),
                    rescored -> {
                        assertHitCount(rescored, 4);

                        assertThat(rescored.getHits().getMaxScore(), equalTo(rescored.getHits().getHits()[0].getScore()));
                        if ("total".equals(scoreMode) || "".equals(scoreMode)) {
                            assertFirstHit(rescored, hasId(String.valueOf(finalI + 1)));
                            assertSecondHit(rescored, hasId(String.valueOf(finalI)));
                            assertThirdHit(rescored, hasId(String.valueOf(finalI + 2)));
                            assertThat(rescored.getHits().getHits()[0].getScore(), equalTo(3.0f * primaryWeight + 7.0f * secondaryWeight));
                            assertThat(rescored.getHits().getHits()[1].getScore(), equalTo(2.0f * primaryWeight + 5.0f * secondaryWeight));
                            assertThat(rescored.getHits().getHits()[2].getScore(), equalTo(5.0f * primaryWeight));
                            assertThat(rescored.getHits().getHits()[3].getScore(), equalTo(0.2f * primaryWeight + 0.0f * secondaryWeight));
                        } else if ("max".equals(scoreMode)) {
                            assertFirstHit(rescored, hasId(String.valueOf(finalI + 1)));
                            assertSecondHit(rescored, hasId(String.valueOf(finalI)));
                            assertThirdHit(rescored, hasId(String.valueOf(finalI + 2)));
                            assertThat(rescored.getHits().getHits()[0].getScore(), equalTo(7.0f * secondaryWeight));
                            assertThat(rescored.getHits().getHits()[1].getScore(), equalTo(5.0f * secondaryWeight));
                            assertThat(rescored.getHits().getHits()[2].getScore(), equalTo(5.0f * primaryWeight));
                            assertThat(rescored.getHits().getHits()[3].getScore(), equalTo(0.2f * primaryWeight));
                        } else if ("min".equals(scoreMode)) {
                            assertFirstHit(rescored, hasId(String.valueOf(finalI + 2)));
                            assertSecondHit(rescored, hasId(String.valueOf(finalI + 1)));
                            assertThirdHit(rescored, hasId(String.valueOf(finalI)));
                            assertThat(rescored.getHits().getHits()[0].getScore(), equalTo(5.0f * primaryWeight));
                            assertThat(rescored.getHits().getHits()[1].getScore(), equalTo(3.0f * primaryWeight));
                            assertThat(rescored.getHits().getHits()[2].getScore(), equalTo(2.0f * primaryWeight));
                            assertThat(rescored.getHits().getHits()[3].getScore(), equalTo(0.0f * secondaryWeight));
                        } else if ("avg".equals(scoreMode)) {
                            assertFirstHit(rescored, hasId(String.valueOf(finalI + 1)));
                            assertSecondHit(rescored, hasId(String.valueOf(finalI + 2)));
                            assertThirdHit(rescored, hasId(String.valueOf(finalI)));
                            assertThat(
                                rescored.getHits().getHits()[0].getScore(),
                                equalTo((3.0f * primaryWeight + 7.0f * secondaryWeight) / 2.0f)
                            );
                            assertThat(rescored.getHits().getHits()[1].getScore(), equalTo(5.0f * primaryWeight));
                            assertThat(
                                rescored.getHits().getHits()[2].getScore(),
                                equalTo((2.0f * primaryWeight + 5.0f * secondaryWeight) / 2.0f)
                            );
                            assertThat(rescored.getHits().getHits()[3].getScore(), equalTo((0.2f * primaryWeight) / 2.0f));
                        } else if ("multiply".equals(scoreMode)) {
                            assertFirstHit(rescored, hasId(String.valueOf(finalI + 1)));
                            assertSecondHit(rescored, hasId(String.valueOf(finalI)));
                            assertThirdHit(rescored, hasId(String.valueOf(finalI + 2)));
                            assertThat(rescored.getHits().getHits()[0].getScore(), equalTo(3.0f * primaryWeight * 7.0f * secondaryWeight));
                            assertThat(rescored.getHits().getHits()[1].getScore(), equalTo(2.0f * primaryWeight * 5.0f * secondaryWeight));
                            assertThat(rescored.getHits().getHits()[2].getScore(), equalTo(5.0f * primaryWeight));
                            assertThat(rescored.getHits().getHits()[3].getScore(), equalTo(0.2f * primaryWeight * 0.0f * secondaryWeight));
                        }
                    }
                );
            }
        }
    }

    public void testMultipleRescores() throws Exception {
        int numDocs = indexRandomNumbers("keyword", 1, true);
        QueryRescorerBuilder eightIsGreat = new QueryRescorerBuilder(
            functionScoreQuery(termQuery("field1", English.intToEnglish(8)), ScoreFunctionBuilders.weightFactorFunction(1000.0f)).boostMode(
                CombineFunction.REPLACE
            )
        ).setScoreMode(QueryRescoreMode.Total);
        QueryRescorerBuilder sevenIsBetter = new QueryRescorerBuilder(
            functionScoreQuery(termQuery("field1", English.intToEnglish(7)), ScoreFunctionBuilders.weightFactorFunction(10000.0f))
                .boostMode(CombineFunction.REPLACE)
        ).setScoreMode(QueryRescoreMode.Total);

        // First set the rescore window large enough that both rescores take effect
        SearchRequestBuilder request = prepareSearch();
        request.addRescorer(eightIsGreat, numDocs).addRescorer(sevenIsBetter, numDocs);
        assertResponse(request, response -> {
            assertFirstHit(response, hasId("7"));
            assertSecondHit(response, hasId("8"));
        });

        // Now squash the second rescore window so it never gets to see a seven
        assertResponse(
            request.setSize(1).clearRescorers().addRescorer(eightIsGreat, numDocs).addRescorer(sevenIsBetter, 1),
            response -> assertFirstHit(response, hasId("8"))
        );
        // We have no idea what the second hit will be because we didn't get a chance to look for seven

        // Now use one rescore to drag the number we're looking for into the window of another
        QueryRescorerBuilder ninetyIsGood = new QueryRescorerBuilder(
            functionScoreQuery(queryStringQuery("*ninety*"), ScoreFunctionBuilders.weightFactorFunction(1000.0f)).boostMode(
                CombineFunction.REPLACE
            )
        ).setScoreMode(QueryRescoreMode.Total);
        QueryRescorerBuilder oneToo = new QueryRescorerBuilder(
            functionScoreQuery(queryStringQuery("*one*"), ScoreFunctionBuilders.weightFactorFunction(1000.0f)).boostMode(
                CombineFunction.REPLACE
            )
        ).setScoreMode(QueryRescoreMode.Total);
        request.clearRescorers().addRescorer(ninetyIsGood, numDocs).addRescorer(oneToo, 10);
        assertResponse(request.setSize(2), response -> {
            assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
            assertFirstHit(response, hasId("91"));
            assertFirstHit(response, hasScore(2001.0f));
            assertSecondHit(response, hasScore(1001.0f)); // Not sure which one it is but it is ninety something
        });
    }

    private int indexRandomNumbers(String analyzer) throws Exception {
        return indexRandomNumbers(analyzer, -1, true);
    }

    private int indexRandomNumbers(String analyzer, int shards, boolean dummyDocs) throws Exception {
        Builder builder = Settings.builder().put(indexSettings());

        if (shards > 0) {
            builder.put(SETTING_NUMBER_OF_SHARDS, shards);
        }

        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field1")
                    .field("analyzer", analyzer)
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            ).setSettings(builder)
        );
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }

        indexRandom(true, dummyDocs, docs);
        ensureGreen();
        return numDocs;
    }

    // #11277
    public void testFromSize() throws Exception {
        assertAcked(prepareCreate("test").setSettings(indexSettings(1, 0)));
        for (int i = 0; i < 5; i++) {
            prepareIndex("test").setId("" + i).setSource("text", "hello world").get();
        }
        refresh();

        SearchRequestBuilder request = prepareSearch();
        request.setQuery(QueryBuilders.termQuery("text", "hello"));
        request.setFrom(1);
        request.setSize(4);
        request.addRescorer(new QueryRescorerBuilder(matchAllQuery()), 50);

        assertResponse(request, response -> assertEquals(4, response.getHits().getHits().length));
    }

    public void testRescorePhaseWithInvalidSort() throws Exception {
        assertAcked(prepareCreate("test"));
        for (int i = 0; i < 5; i++) {
            prepareIndex("test").setId("" + i).setSource("number", 0).get();
        }
        refresh();

        Exception exc = expectThrows(
            Exception.class,
            prepareSearch().addSort(SortBuilders.fieldSort("number"))
                .setTrackScores(true)
                .addRescorer(new QueryRescorerBuilder(matchAllQuery()), 50)
        );
        assertNotNull(exc.getCause());
        assertThat(exc.getCause().getMessage(), containsString("Cannot use [sort] option in conjunction with [rescore]."));

        exc = expectThrows(
            Exception.class,
            prepareSearch().addSort(SortBuilders.fieldSort("number"))
                .addSort(SortBuilders.scoreSort())
                .setTrackScores(true)
                .addRescorer(new QueryRescorerBuilder(matchAllQuery()), 50)
        );
        assertNotNull(exc.getCause());
        assertThat(exc.getCause().getMessage(), containsString("Cannot use [sort] option in conjunction with [rescore]."));

        assertResponse(
            prepareSearch().addSort(SortBuilders.scoreSort())
                .setTrackScores(true)
                .addRescorer(new QueryRescorerBuilder(matchAllQuery()).setRescoreQueryWeight(100.0f), 50),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(5L));
                assertThat(response.getHits().getHits().length, equalTo(5));
                for (SearchHit hit : response.getHits().getHits()) {
                    assertThat(hit.getScore(), equalTo(101f));
                }
            }
        );
    }

    record GroupDoc(String id, String group, float firstPassScore, float secondPassScore, boolean shouldFilter) {}

    public void testRescoreAfterCollapse() throws Exception {
        assertAcked(prepareCreate("test").setMapping("group", "type=keyword", "shouldFilter", "type=boolean"));
        ensureGreen("test");
        GroupDoc[] groupDocs = new GroupDoc[] {
            new GroupDoc("1", "c", 200, 1, false),
            new GroupDoc("2", "a", 1, 10, true),
            new GroupDoc("3", "b", 2, 30, false),
            new GroupDoc("4", "c", 1, 1000, false),
            // should be highest on rescore, but filtered out during collapse
            new GroupDoc("5", "b", 1, 40, false),
            new GroupDoc("6", "a", 2, 20, false) };
        List<IndexRequestBuilder> requests = new ArrayList<>();
        for (var groupDoc : groupDocs) {
            requests.add(
                client().prepareIndex("test")
                    .setId(groupDoc.id())
                    .setRouting(groupDoc.group())
                    .setSource(
                        "group",
                        groupDoc.group(),
                        "firstPassScore",
                        groupDoc.firstPassScore(),
                        "secondPassScore",
                        groupDoc.secondPassScore(),
                        "shouldFilter",
                        groupDoc.shouldFilter()
                    )
            );
        }
        indexRandom(true, requests);

        var request = client().prepareSearch("test")
            .setQuery(fieldValueScoreQuery("firstPassScore"))
            .addRescorer(new QueryRescorerBuilder(fieldValueScoreQuery("secondPassScore")))
            .setCollapse(new CollapseBuilder("group"));
        assertResponse(request, resp -> {
            assertThat(resp.getHits().getTotalHits().value(), equalTo(5L));
            assertThat(resp.getHits().getHits().length, equalTo(3));

            SearchHit hit1 = resp.getHits().getAt(0);
            assertThat(hit1.getId(), equalTo("1"));
            assertThat(hit1.getScore(), equalTo(201F));
            assertThat(hit1.field("group").getValues().size(), equalTo(1));
            assertThat(hit1.field("group").getValues().get(0), equalTo("c"));

            SearchHit hit2 = resp.getHits().getAt(1);
            assertThat(hit2.getId(), equalTo("3"));
            assertThat(hit2.getScore(), equalTo(32F));
            assertThat(hit2.field("group").getValues().size(), equalTo(1));
            assertThat(hit2.field("group").getValues().get(0), equalTo("b"));

            SearchHit hit3 = resp.getHits().getAt(2);
            assertThat(hit3.getId(), equalTo("6"));
            assertThat(hit3.getScore(), equalTo(22F));
            assertThat(hit3.field("group").getValues().size(), equalTo(1));
            assertThat(hit3.field("group").getValues().get(0), equalTo("a"));
        });
    }

    public void testRescoreAfterCollapseRandom() throws Exception {
        assertAcked(prepareCreate("test").setMapping("group", "type=keyword", "shouldFilter", "type=boolean"));
        ensureGreen("test");
        int numGroups = randomIntBetween(1, 100);
        int numDocs = atLeast(100);
        GroupDoc[] groups = new GroupDoc[numGroups];
        int numHits = 0;
        List<IndexRequestBuilder> requests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            int group = randomIntBetween(0, numGroups - 1);
            boolean shouldFilter = rarely();
            String id = randomUUID();
            float firstPassScore = randomFloat();
            float secondPassScore = randomFloat();
            float bestScore = groups[group] == null ? -1 : groups[group].firstPassScore;
            var groupDoc = new GroupDoc(id, Integer.toString(group), firstPassScore, secondPassScore, shouldFilter);
            if (shouldFilter == false) {
                if (firstPassScore == bestScore) {
                    // avoid tiebreaker
                    continue;
                }

                numHits++;
                if (firstPassScore > bestScore) {
                    groups[group] = groupDoc;
                }
            }
            requests.add(
                client().prepareIndex("test")
                    .setId(groupDoc.id())
                    .setRouting(groupDoc.group())
                    .setSource(
                        "group",
                        groupDoc.group(),
                        "firstPassScore",
                        groupDoc.firstPassScore(),
                        "secondPassScore",
                        groupDoc.secondPassScore(),
                        "shouldFilter",
                        groupDoc.shouldFilter()
                    )
            );
        }
        indexRandom(true, requests);

        GroupDoc[] sortedGroups = Arrays.stream(groups)
            .filter(g -> g != null)
            .sorted(Comparator.comparingDouble(GroupDoc::secondPassScore).reversed())
            .toArray(GroupDoc[]::new);

        var request = client().prepareSearch("test")
            .setQuery(fieldValueScoreQuery("firstPassScore"))
            .addRescorer(new QueryRescorerBuilder(fieldValueScoreQuery("secondPassScore")).setQueryWeight(0f).windowSize(numGroups))
            .setCollapse(new CollapseBuilder("group"))
            .setSize(Math.min(numGroups, 10));
        long expectedNumHits = numHits;
        assertResponse(request, resp -> {
            assertThat(resp.getHits().getTotalHits().value(), equalTo(expectedNumHits));
            for (int pos = 0; pos < resp.getHits().getHits().length; pos++) {
                SearchHit hit = resp.getHits().getAt(pos);
                assertThat(hit.getId(), equalTo(sortedGroups[pos].id()));
                String group = hit.field("group").getValue();
                assertThat(group, equalTo(sortedGroups[pos].group()));
                assertThat(hit.getScore(), equalTo(sortedGroups[pos].secondPassScore));
            }
        });
    }

    public void testRescoreWithTimeout() throws Exception {
        // no dummy docs since merges can change scores while we run queries.
        int numDocs = indexRandomNumbers("whitespace", -1, false);

        String intToEnglish = English.intToEnglish(between(0, numDocs - 1));
        String query = intToEnglish.split(" ")[0];
        assertResponse(
            prepareSearch().setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("field1", query).operator(Operator.OR))
                .setSize(10)
                .addRescorer(new QueryRescorerBuilder(functionScoreQuery(new TestTimedScoreFunctionBuilder())).windowSize(100))
                .setTimeout(TimeValue.timeValueMillis(10)),
            r -> assertTrue(r.isTimedOut())
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestTimedQueryPlugin.class);
    }

    private QueryBuilder fieldValueScoreQuery(String scoreField) {
        return functionScoreQuery(termQuery("shouldFilter", false), ScoreFunctionBuilders.fieldValueFactorFunction(scoreField)).boostMode(
            CombineFunction.REPLACE
        );
    }

    public static class TestTimedQueryPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<ScoreFunctionSpec<?>> getScoreFunctions() {
            return List.of(
                new ScoreFunctionSpec<>(
                    new ParseField("timed"),
                    TestTimedScoreFunctionBuilder::new,
                    p -> new TestTimedScoreFunctionBuilder()
                )
            );
        }
    }

    static class TestTimedScoreFunctionBuilder extends ScoreFunctionBuilder<TestTimedScoreFunctionBuilder> {
        private final long time = 500;

        TestTimedScoreFunctionBuilder() {}

        TestTimedScoreFunctionBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        public String getName() {
            return "timed";
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) {}

        @Override
        protected boolean doEquals(TestTimedScoreFunctionBuilder functionBuilder) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        protected ScoreFunction doToFunction(SearchExecutionContext context) throws IOException {
            return new ScoreFunction(REPLACE) {
                @Override
                public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
                    return new LeafScoreFunction() {
                        @Override
                        public double score(int docId, float subQueryScore) {
                            try {
                                Thread.sleep(time);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return time;
                        }

                        @Override
                        public Explanation explainScore(int docId, Explanation subQueryScore) {
                            return null;
                        }
                    };
                }

                @Override
                public boolean needsScores() {
                    return true;
                }

                @Override
                protected boolean doEquals(ScoreFunction other) {
                    return false;
                }

                @Override
                protected int doHashCode() {
                    return 0;
                }
            };
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }
    }
}
