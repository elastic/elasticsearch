/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.functionscore;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.tests.util.English;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.rescore.QueryRescoreMode;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.Arrays;
import java.util.Comparator;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
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
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
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
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("f", Integer.toString(i)).get();
        }
        refresh();

        int numShards = getNumShards("test").numPrimaries;
        for (int j = 0; j < iters; j++) {
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .setRescorer(
                    new QueryRescorerBuilder(
                        functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.weightFactorFunction(100)).boostMode(
                            CombineFunction.REPLACE
                        )
                    ).setQueryWeight(0.0f).setRescoreQueryWeight(1.0f),
                    1
                )
                .setSize(randomIntBetween(2, 10))
                .get();
            assertSearchResponse(searchResponse);
            assertFirstHit(searchResponse, hasScore(100.f));
            int numDocsWith100AsAScore = 0;
            for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
                float score = searchResponse.getHits().getHits()[i].getScore();
                if (score == 100f) {
                    numDocsWith100AsAScore += 1;
                }
            }
            assertThat(searchResponse.getHits().getMaxScore(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
            // we cannot assert that they are equal since some shards might not have docs at all
            assertThat(numDocsWith100AsAScore, lessThanOrEqualTo(numShards));
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

        client().prepareIndex("test").setId("1").setSource("field1", "the quick brown fox").get();
        client().prepareIndex("test").setId("2").setSource("field1", "the quick lazy huge brown fox jumps over the tree ").get();
        client().prepareIndex("test")
            .setId("3")
            .setSource("field1", "quick huge brown", "field2", "the quick lazy huge brown fox jumps over the tree")
            .get();
        refresh();
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
            .setRescorer(
                new QueryRescorerBuilder(matchPhraseQuery("field1", "quick brown").slop(2).boost(4.0f)).setRescoreQueryWeight(2),
                5
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getMaxScore(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("2"));

        searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
            .setRescorer(new QueryRescorerBuilder(matchPhraseQuery("field1", "the quick brown").slop(3)), 5)
            .get();

        assertHitCount(searchResponse, 3);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
            .setRescorer(new QueryRescorerBuilder(matchPhraseQuery("field1", "the quick brown")), 5)
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getMaxScore(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));
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

        assertAcked(
            client().admin().indices().prepareCreate("test").setMapping(mapping).setSettings(builder.put("index.number_of_shards", 1))
        );

        client().prepareIndex("test").setId("1").setSource("field1", "massachusetts avenue boston massachusetts").get();
        client().prepareIndex("test").setId("2").setSource("field1", "lexington avenue boston massachusetts").get();
        client().prepareIndex("test").setId("3").setSource("field1", "boston avenue lexington massachusetts").get();
        client().admin().indices().prepareRefresh("test").get();
        client().prepareIndex("test").setId("4").setSource("field1", "boston road lexington massachusetts").get();
        client().prepareIndex("test").setId("5").setSource("field1", "lexington street lexington massachusetts").get();
        client().prepareIndex("test").setId("6").setSource("field1", "massachusetts avenue lexington massachusetts").get();
        client().prepareIndex("test").setId("7").setSource("field1", "bosten street san franciso california").get();
        client().admin().indices().prepareRefresh("test").get();
        client().prepareIndex("test").setId("8").setSource("field1", "hollywood boulevard los angeles california").get();
        client().prepareIndex("test").setId("9").setSource("field1", "1st street boston massachussetts").get();
        client().prepareIndex("test").setId("10").setSource("field1", "1st street boston massachusetts").get();
        client().admin().indices().prepareRefresh("test").get();
        client().prepareIndex("test").setId("11").setSource("field1", "2st street boston massachusetts").get();
        client().prepareIndex("test").setId("12").setSource("field1", "3st street boston massachusetts").get();
        client().admin().indices().prepareRefresh("test").get();
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "lexington avenue massachusetts").operator(Operator.OR))
            .setFrom(0)
            .setSize(5)
            .setRescorer(
                new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(0.6f)
                    .setRescoreQueryWeight(2.0f),
                20
            )
            .get();

        assertThat(searchResponse.getHits().getHits().length, equalTo(5));
        assertHitCount(searchResponse, 9);
        assertFirstHit(searchResponse, hasId("2"));
        assertSecondHit(searchResponse, hasId("6"));
        assertThirdHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "lexington avenue massachusetts").operator(Operator.OR))
            .setFrom(0)
            .setSize(5)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setRescorer(
                new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(0.6f)
                    .setRescoreQueryWeight(2.0f),
                20
            )
            .get();

        assertThat(searchResponse.getHits().getHits().length, equalTo(5));
        assertHitCount(searchResponse, 9);
        assertThat(searchResponse.getHits().getMaxScore(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
        assertFirstHit(searchResponse, hasId("2"));
        assertSecondHit(searchResponse, hasId("6"));
        assertThirdHit(searchResponse, hasId("3"));

        // Make sure non-zero from works:
        searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "lexington avenue massachusetts").operator(Operator.OR))
            .setFrom(2)
            .setSize(5)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setRescorer(
                new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(0.6f)
                    .setRescoreQueryWeight(2.0f),
                20
            )
            .get();

        assertThat(searchResponse.getHits().getHits().length, equalTo(5));
        assertHitCount(searchResponse, 9);
        assertThat(searchResponse.getHits().getMaxScore(), greaterThan(searchResponse.getHits().getHits()[0].getScore()));
        assertFirstHit(searchResponse, hasId("3"));
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

        assertAcked(
            client().admin().indices().prepareCreate("test").setMapping(mapping).setSettings(builder.put("index.number_of_shards", 1))
        );

        client().prepareIndex("test").setId("3").setSource("field1", "massachusetts").get();
        client().prepareIndex("test").setId("6").setSource("field1", "massachusetts avenue lexington massachusetts").get();
        client().admin().indices().prepareRefresh("test").get();
        client().prepareIndex("test").setId("1").setSource("field1", "lexington massachusetts avenue").get();
        client().prepareIndex("test").setId("2").setSource("field1", "lexington avenue boston massachusetts road").get();
        client().admin().indices().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "massachusetts"))
            .setFrom(0)
            .setSize(5)
            .get();
        assertThat(searchResponse.getHits().getHits().length, equalTo(4));
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().getMaxScore(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
        assertFirstHit(searchResponse, hasId("3"));
        assertSecondHit(searchResponse, hasId("6"));
        assertThirdHit(searchResponse, hasId("1"));
        assertFourthHit(searchResponse, hasId("2"));

        // Now, rescore only top 2 hits w/ proximity:
        searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "massachusetts"))
            .setFrom(0)
            .setSize(5)
            .setRescorer(
                new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(0.6f)
                    .setRescoreQueryWeight(2.0f),
                2
            )
            .get();
        // Only top 2 hits were re-ordered:
        assertThat(searchResponse.getHits().getHits().length, equalTo(4));
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().getMaxScore(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
        assertFirstHit(searchResponse, hasId("6"));
        assertSecondHit(searchResponse, hasId("3"));
        assertThirdHit(searchResponse, hasId("1"));
        assertFourthHit(searchResponse, hasId("2"));

        // Now, rescore only top 3 hits w/ proximity:
        searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "massachusetts"))
            .setFrom(0)
            .setSize(5)
            .setRescorer(
                new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(0.6f)
                    .setRescoreQueryWeight(2.0f),
                3
            )
            .get();

        // Only top 3 hits were re-ordered:
        assertThat(searchResponse.getHits().getHits().length, equalTo(4));
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().getMaxScore(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("6"));
        assertThirdHit(searchResponse, hasId("3"));
        assertFourthHit(searchResponse, hasId("2"));
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

        assertAcked(
            client().admin().indices().prepareCreate("test").setMapping(mapping).setSettings(builder.put("index.number_of_shards", 1))
        );

        client().prepareIndex("test").setId("3").setSource("field1", "massachusetts").get();
        client().prepareIndex("test").setId("6").setSource("field1", "massachusetts avenue lexington massachusetts").get();
        client().admin().indices().prepareRefresh("test").get();
        client().prepareIndex("test").setId("1").setSource("field1", "lexington massachusetts avenue").get();
        client().prepareIndex("test").setId("2").setSource("field1", "lexington avenue boston massachusetts road").get();
        client().admin().indices().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "massachusetts").operator(Operator.OR))
            .setFrom(0)
            .setSize(5)
            .get();
        assertThat(searchResponse.getHits().getHits().length, equalTo(4));
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().getMaxScore(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
        assertFirstHit(searchResponse, hasId("3"));
        assertSecondHit(searchResponse, hasId("6"));
        assertThirdHit(searchResponse, hasId("1"));
        assertFourthHit(searchResponse, hasId("2"));

        // Now, penalizing rescore (nothing matches the rescore query):
        searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("field1", "massachusetts").operator(Operator.OR))
            .setFrom(0)
            .setSize(5)
            .setRescorer(
                new QueryRescorerBuilder(matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3)).setQueryWeight(1.0f)
                    .setRescoreQueryWeight(-1f),
                3
            )
            .get();

        // 6 and 1 got worse, and then the hit (2) outside the rescore window were sorted ahead:
        assertThat(searchResponse.getHits().getMaxScore(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
        assertFirstHit(searchResponse, hasId("3"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("6"));
        assertFourthHit(searchResponse, hasId("1"));
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
        assertThat(leftHits.getTotalHits().value, equalTo(rightHits.getTotalHits().value));
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
            SearchResponse rescored = client().prepareSearch()
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPreference("test") // ensure we hit the same shards for tie-breaking
                .setQuery(QueryBuilders.matchQuery("field1", query).operator(Operator.OR))
                .setFrom(0)
                .setSize(resultSize)
                .setRescorer(
                    new QueryRescorerBuilder(constantScoreQuery(matchPhraseQuery("field1", intToEnglish).slop(3))).setQueryWeight(1.0f)
                        // no weight - so we basically use the same score as the actual query
                        .setRescoreQueryWeight(0.0f),
                    rescoreWindow
                )
                .get();

            SearchResponse plain = client().prepareSearch()
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPreference("test") // ensure we hit the same shards for tie-breaking
                .setQuery(QueryBuilders.matchQuery("field1", query).operator(Operator.OR))
                .setFrom(0)
                .setSize(resultSize)
                .get();

            // check equivalence
            assertEquivalent(query, plain, rescored);

            rescored = client().prepareSearch()
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPreference("test") // ensure we hit the same shards for tie-breaking
                .setQuery(QueryBuilders.matchQuery("field1", query).operator(Operator.OR))
                .setFrom(0)
                .setSize(resultSize)
                .setRescorer(
                    new QueryRescorerBuilder(constantScoreQuery(matchPhraseQuery("field1", "not in the index").slop(3))).setQueryWeight(
                        1.0f
                    ).setRescoreQueryWeight(1.0f),
                    rescoreWindow
                )
                .get();
            // check equivalence
            assertEquivalent(query, plain, rescored);
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
        client().prepareIndex("test").setId("1").setSource("field1", "the quick brown fox").get();
        client().prepareIndex("test").setId("2").setSource("field1", "the quick lazy huge brown fox jumps over the tree").get();
        client().prepareIndex("test")
            .setId("3")
            .setSource("field1", "quick huge brown", "field2", "the quick lazy huge brown fox jumps over the tree")
            .get();
        refresh();

        {
            SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
                .setRescorer(
                    new QueryRescorerBuilder(matchPhraseQuery("field1", "the quick brown").slop(2).boost(4.0f)).setQueryWeight(0.5f)
                        .setRescoreQueryWeight(0.4f),
                    5
                )
                .setExplain(true)
                .get();
            assertHitCount(searchResponse, 3);
            assertFirstHit(searchResponse, hasId("1"));
            assertSecondHit(searchResponse, hasId("2"));
            assertThirdHit(searchResponse, hasId("3"));

            for (int i = 0; i < 3; i++) {
                assertThat(searchResponse.getHits().getAt(i).getExplanation(), notNullValue());
                assertThat(searchResponse.getHits().getAt(i).getExplanation().isMatch(), equalTo(true));
                assertThat(searchResponse.getHits().getAt(i).getExplanation().getDetails().length, equalTo(2));
                assertThat(searchResponse.getHits().getAt(i).getExplanation().getDetails()[0].isMatch(), equalTo(true));
                if (i == 2) {
                    assertThat(searchResponse.getHits().getAt(i).getExplanation().getDetails()[1].getValue(), equalTo(0.5f));
                } else {
                    assertThat(searchResponse.getHits().getAt(i).getExplanation().getDescription(), equalTo("sum of:"));
                    assertThat(
                        searchResponse.getHits().getAt(i).getExplanation().getDetails()[0].getDetails()[1].getValue(),
                        equalTo(0.5f)
                    );
                    assertThat(
                        searchResponse.getHits().getAt(i).getExplanation().getDetails()[1].getDetails()[1].getValue(),
                        equalTo(0.4f)
                    );
                }
            }
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

            SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
                .setRescorer(innerRescoreQuery, 5)
                .setExplain(true)
                .get();
            assertHitCount(searchResponse, 3);
            assertFirstHit(searchResponse, hasId("1"));
            assertSecondHit(searchResponse, hasId("2"));
            assertThirdHit(searchResponse, hasId("3"));

            for (int j = 0; j < 3; j++) {
                assertThat(searchResponse.getHits().getAt(j).getExplanation().getDescription(), equalTo(descriptionModes[innerMode]));
            }

            for (int outerMode = 0; outerMode < scoreModes.length; outerMode++) {
                QueryRescorerBuilder outerRescoreQuery = new QueryRescorerBuilder(matchQuery("field1", "the quick brown").boost(4.0f))
                    .setQueryWeight(0.5f)
                    .setRescoreQueryWeight(0.4f);

                if ("".equals(scoreModes[outerMode]) == false) {
                    outerRescoreQuery.setScoreMode(QueryRescoreMode.fromString(scoreModes[outerMode]));
                }

                searchResponse = client().prepareSearch()
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR))
                    .addRescorer(innerRescoreQuery, 5)
                    .addRescorer(outerRescoreQuery.windowSize(10))
                    .setExplain(true)
                    .get();
                assertHitCount(searchResponse, 3);
                assertFirstHit(searchResponse, hasId("1"));
                assertSecondHit(searchResponse, hasId("2"));
                assertThirdHit(searchResponse, hasId("3"));

                for (int j = 0; j < 3; j++) {
                    Explanation explanation = searchResponse.getHits().getAt(j).getExplanation();
                    assertThat(explanation.getDescription(), equalTo(descriptionModes[outerMode]));
                    assertThat(explanation.getDetails()[0].getDetails()[0].getDescription(), equalTo(descriptionModes[innerMode]));
                }
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

                SearchResponse rescored = client().prepareSearch()
                    .setPreference("test") // ensure we hit the same shards for tie-breaking
                    .setFrom(0)
                    .setSize(10)
                    .setQuery(query)
                    .setRescorer(rescoreQuery, 50)
                    .get();

                assertHitCount(rescored, 4);

                assertThat(rescored.getHits().getMaxScore(), equalTo(rescored.getHits().getHits()[0].getScore()));
                if ("total".equals(scoreMode) || "".equals(scoreMode)) {
                    assertFirstHit(rescored, hasId(String.valueOf(i + 1)));
                    assertSecondHit(rescored, hasId(String.valueOf(i)));
                    assertThirdHit(rescored, hasId(String.valueOf(i + 2)));
                    assertThat(rescored.getHits().getHits()[0].getScore(), equalTo(3.0f * primaryWeight + 7.0f * secondaryWeight));
                    assertThat(rescored.getHits().getHits()[1].getScore(), equalTo(2.0f * primaryWeight + 5.0f * secondaryWeight));
                    assertThat(rescored.getHits().getHits()[2].getScore(), equalTo(5.0f * primaryWeight));
                    assertThat(rescored.getHits().getHits()[3].getScore(), equalTo(0.2f * primaryWeight + 0.0f * secondaryWeight));
                } else if ("max".equals(scoreMode)) {
                    assertFirstHit(rescored, hasId(String.valueOf(i + 1)));
                    assertSecondHit(rescored, hasId(String.valueOf(i)));
                    assertThirdHit(rescored, hasId(String.valueOf(i + 2)));
                    assertThat(rescored.getHits().getHits()[0].getScore(), equalTo(7.0f * secondaryWeight));
                    assertThat(rescored.getHits().getHits()[1].getScore(), equalTo(5.0f * secondaryWeight));
                    assertThat(rescored.getHits().getHits()[2].getScore(), equalTo(5.0f * primaryWeight));
                    assertThat(rescored.getHits().getHits()[3].getScore(), equalTo(0.2f * primaryWeight));
                } else if ("min".equals(scoreMode)) {
                    assertFirstHit(rescored, hasId(String.valueOf(i + 2)));
                    assertSecondHit(rescored, hasId(String.valueOf(i + 1)));
                    assertThirdHit(rescored, hasId(String.valueOf(i)));
                    assertThat(rescored.getHits().getHits()[0].getScore(), equalTo(5.0f * primaryWeight));
                    assertThat(rescored.getHits().getHits()[1].getScore(), equalTo(3.0f * primaryWeight));
                    assertThat(rescored.getHits().getHits()[2].getScore(), equalTo(2.0f * primaryWeight));
                    assertThat(rescored.getHits().getHits()[3].getScore(), equalTo(0.0f * secondaryWeight));
                } else if ("avg".equals(scoreMode)) {
                    assertFirstHit(rescored, hasId(String.valueOf(i + 1)));
                    assertSecondHit(rescored, hasId(String.valueOf(i + 2)));
                    assertThirdHit(rescored, hasId(String.valueOf(i)));
                    assertThat(rescored.getHits().getHits()[0].getScore(), equalTo((3.0f * primaryWeight + 7.0f * secondaryWeight) / 2.0f));
                    assertThat(rescored.getHits().getHits()[1].getScore(), equalTo(5.0f * primaryWeight));
                    assertThat(rescored.getHits().getHits()[2].getScore(), equalTo((2.0f * primaryWeight + 5.0f * secondaryWeight) / 2.0f));
                    assertThat(rescored.getHits().getHits()[3].getScore(), equalTo((0.2f * primaryWeight) / 2.0f));
                } else if ("multiply".equals(scoreMode)) {
                    assertFirstHit(rescored, hasId(String.valueOf(i + 1)));
                    assertSecondHit(rescored, hasId(String.valueOf(i)));
                    assertThirdHit(rescored, hasId(String.valueOf(i + 2)));
                    assertThat(rescored.getHits().getHits()[0].getScore(), equalTo(3.0f * primaryWeight * 7.0f * secondaryWeight));
                    assertThat(rescored.getHits().getHits()[1].getScore(), equalTo(2.0f * primaryWeight * 5.0f * secondaryWeight));
                    assertThat(rescored.getHits().getHits()[2].getScore(), equalTo(5.0f * primaryWeight));
                    assertThat(rescored.getHits().getHits()[3].getScore(), equalTo(0.2f * primaryWeight * 0.0f * secondaryWeight));
                }
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
        SearchRequestBuilder request = client().prepareSearch();
        request.addRescorer(eightIsGreat, numDocs).addRescorer(sevenIsBetter, numDocs);
        SearchResponse response = request.get();
        assertFirstHit(response, hasId("7"));
        assertSecondHit(response, hasId("8"));

        // Now squash the second rescore window so it never gets to see a seven
        response = request.setSize(1).clearRescorers().addRescorer(eightIsGreat, numDocs).addRescorer(sevenIsBetter, 1).get();
        assertFirstHit(response, hasId("8"));
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
        response = request.setSize(2).get();
        assertThat(response.getHits().getMaxScore(), equalTo(response.getHits().getHits()[0].getScore()));
        assertFirstHit(response, hasId("91"));
        assertFirstHit(response, hasScore(2001.0f));
        assertSecondHit(response, hasScore(1001.0f)); // Not sure which one it is but it is ninety something
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
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }

        indexRandom(true, dummyDocs, docs);
        ensureGreen();
        return numDocs;
    }

    // #11277
    public void testFromSize() throws Exception {
        Builder settings = Settings.builder();
        settings.put(SETTING_NUMBER_OF_SHARDS, 1);
        settings.put(SETTING_NUMBER_OF_REPLICAS, 0);
        assertAcked(prepareCreate("test").setSettings(settings));
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test").setId("" + i).setSource("text", "hello world").get();
        }
        refresh();

        SearchRequestBuilder request = client().prepareSearch();
        request.setQuery(QueryBuilders.termQuery("text", "hello"));
        request.setFrom(1);
        request.setSize(4);
        request.addRescorer(new QueryRescorerBuilder(matchAllQuery()), 50);

        assertEquals(4, request.get().getHits().getHits().length);
    }

    public void testRescorePhaseWithInvalidSort() throws Exception {
        assertAcked(prepareCreate("test"));
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test").setId("" + i).setSource("number", 0).get();
        }
        refresh();

        Exception exc = expectThrows(
            Exception.class,
            () -> client().prepareSearch()
                .addSort(SortBuilders.fieldSort("number"))
                .setTrackScores(true)
                .addRescorer(new QueryRescorerBuilder(matchAllQuery()), 50)
                .get()
        );
        assertNotNull(exc.getCause());
        assertThat(exc.getCause().getMessage(), containsString("Cannot use [sort] option in conjunction with [rescore]."));

        exc = expectThrows(
            Exception.class,
            () -> client().prepareSearch()
                .addSort(SortBuilders.fieldSort("number"))
                .addSort(SortBuilders.scoreSort())
                .setTrackScores(true)
                .addRescorer(new QueryRescorerBuilder(matchAllQuery()), 50)
                .get()
        );
        assertNotNull(exc.getCause());
        assertThat(exc.getCause().getMessage(), containsString("Cannot use [sort] option in conjunction with [rescore]."));

        SearchResponse resp = client().prepareSearch()
            .addSort(SortBuilders.scoreSort())
            .setTrackScores(true)
            .addRescorer(new QueryRescorerBuilder(matchAllQuery()).setRescoreQueryWeight(100.0f), 50)
            .get();
        assertThat(resp.getHits().getTotalHits().value, equalTo(5L));
        assertThat(resp.getHits().getHits().length, equalTo(5));
        for (SearchHit hit : resp.getHits().getHits()) {
            assertThat(hit.getScore(), equalTo(101f));
        }
    }
}
