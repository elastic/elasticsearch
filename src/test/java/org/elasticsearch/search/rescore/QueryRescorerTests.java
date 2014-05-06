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

package org.elasticsearch.search.rescore;



import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.English;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.rescore.RescoreBuilder.QueryRescorer;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class QueryRescorerTests extends ElasticsearchIntegrationTest {

    @Test
    public void testEnforceWindowSize() {
        createIndex("test");
        // this
        int iters = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < iters; i ++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("f", Integer.toString(i)).execute().actionGet();
        }
        refresh();

        int numShards = getNumShards("test").numPrimaries;
        for (int j = 0 ; j < iters; j++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setRescorer(RescoreBuilder.queryRescorer(
                            QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery())
                                    .boostMode("replace").add(ScoreFunctionBuilders.factorFunction(100))).setQueryWeight(0.0f).setRescoreQueryWeight(1.0f))
                    .setRescoreWindow(1).setSize(randomIntBetween(2,10)).execute().actionGet();
            assertFirstHit(searchResponse, hasScore(100.f));
            int numPending100 = numShards;
            for (int i = 0; i < searchResponse.getHits().hits().length; i++) {
                float score = searchResponse.getHits().hits()[i].getScore();
                if  (score == 100f) {
                    assertThat(numPending100--, greaterThanOrEqualTo(0));
                } else {
                    assertThat(numPending100, equalTo(0));
                }
            }
        }

    }

    @Test
    public void testRescorePhrase() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("analyzer", "whitespace").field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put(indexSettings()).put("index.number_of_shards", 2)));

        client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "the quick lazy huge brown fox jumps over the tree").execute()
                .actionGet();
        client().prepareIndex("test", "type1", "3")
                .setSource("field1", "quick huge brown", "field2", "the quick lazy huge brown fox jumps over the tree").execute()
                .actionGet();
        refresh();
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(MatchQueryBuilder.Operator.OR))
                .setRescorer(RescoreBuilder.queryRescorer(QueryBuilders.matchPhraseQuery("field1", "quick brown").slop(2).boost(4.0f)))
                .setRescoreWindow(5).execute().actionGet();

        assertThat(searchResponse.getHits().totalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(MatchQueryBuilder.Operator.OR))
                .setRescorer(RescoreBuilder.queryRescorer(QueryBuilders.matchPhraseQuery("field1", "the quick brown").slop(3)))
                .setRescoreWindow(5).execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch()
                .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(MatchQueryBuilder.Operator.OR))
                .setRescorer(RescoreBuilder.queryRescorer((QueryBuilders.matchPhraseQuery("field1", "the quick brown"))))
                .setRescoreWindow(5).execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertFirstHit(searchResponse, hasId("1"));
        assertSecondHit(searchResponse, hasId("2"));
        assertThirdHit(searchResponse, hasId("3"));
    }

    @Test
    public void testMoreDocs() throws Exception {
        Builder builder = ImmutableSettings.builder();
        builder.put("index.analysis.analyzer.synonym.tokenizer", "whitespace");
        builder.putArray("index.analysis.analyzer.synonym.filter", "synonym", "lowercase");
        builder.put("index.analysis.filter.synonym.type", "synonym");
        builder.putArray("index.analysis.filter.synonym.synonyms", "ave => ave, avenue", "street => str, street");

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("field1").field("type", "string").field("index_analyzer", "whitespace").field("search_analyzer", "synonym")
                .endObject().endObject().endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", mapping).setSettings(builder.put("index.number_of_shards", 1)));

        client().prepareIndex("test", "type1", "1").setSource("field1", "massachusetts avenue boston massachusetts").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "lexington avenue boston massachusetts").execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource("field1", "boston avenue lexington massachusetts").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        client().prepareIndex("test", "type1", "4").setSource("field1", "boston road lexington massachusetts").execute().actionGet();
        client().prepareIndex("test", "type1", "5").setSource("field1", "lexington street lexington massachusetts").execute().actionGet();
        client().prepareIndex("test", "type1", "6").setSource("field1", "massachusetts avenue lexington massachusetts").execute().actionGet();
        client().prepareIndex("test", "type1", "7").setSource("field1", "bosten street san franciso california").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        client().prepareIndex("test", "type1", "8").setSource("field1", "hollywood boulevard los angeles california").execute().actionGet();
        client().prepareIndex("test", "type1", "9").setSource("field1", "1st street boston massachussetts").execute().actionGet();
        client().prepareIndex("test", "type1", "10").setSource("field1", "1st street boston massachusetts").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        client().prepareIndex("test", "type1", "11").setSource("field1", "2st street boston massachusetts").execute().actionGet();
        client().prepareIndex("test", "type1", "12").setSource("field1", "3st street boston massachusetts").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        SearchResponse searchResponse = client()
                .prepareSearch()
                .setQuery(QueryBuilders.matchQuery("field1", "lexington avenue massachusetts").operator(MatchQueryBuilder.Operator.OR))
                .setFrom(0)
                .setSize(5)
                .setRescorer(
                        RescoreBuilder.queryRescorer(QueryBuilders.matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3))
                                .setQueryWeight(0.6f).setRescoreQueryWeight(2.0f)).setRescoreWindow(20).execute().actionGet();

        assertThat(searchResponse.getHits().hits().length, equalTo(5));
        assertHitCount(searchResponse, 9);
        assertFirstHit(searchResponse, hasId("2"));
        assertSecondHit(searchResponse, hasId("6"));
        assertThirdHit(searchResponse, hasId("3"));

        searchResponse = client()
                .prepareSearch()
                .setQuery(QueryBuilders.matchQuery("field1", "lexington avenue massachusetts").operator(MatchQueryBuilder.Operator.OR))
                .setFrom(0)
                .setSize(5)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setRescorer(
                        RescoreBuilder.queryRescorer(QueryBuilders.matchPhraseQuery("field1", "lexington avenue massachusetts").slop(3))
                                .setQueryWeight(0.6f).setRescoreQueryWeight(2.0f)).setRescoreWindow(20).execute().actionGet();

        assertThat(searchResponse.getHits().hits().length, equalTo(5));
        assertHitCount(searchResponse, 9);
        assertFirstHit(searchResponse, hasId("2"));
        assertSecondHit(searchResponse, hasId("6"));
        assertThirdHit(searchResponse, hasId("3"));
    }

    // Comparator that sorts hits and rescored hits in the same way.
    // The rescore uses the docId as tie, while regular search uses the slot the hit is in as a tie if score
    // and shard id are equal during merging shard results.
    // This comparator uses a custom tie in case the scores are equal, so that both regular hits and rescored hits
    // are sorted equally. This is fine since tests only care about the fact the scores should be equal, not ordering.
    private final static Comparator<SearchHit> searchHitsComparator = new Comparator<SearchHit>() {
        @Override
        public int compare(SearchHit hit1, SearchHit hit2) {
            int cmp = Float.compare(hit2.getScore(), hit1.getScore());
            if (cmp == 0) {
                return hit1.id().compareTo(hit2.id());
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
        assertThat(leftHits.getTotalHits(), equalTo(rightHits.getTotalHits()));
        assertThat(leftHits.getHits().length, equalTo(rightHits.getHits().length));
        SearchHit[] hits = leftHits.getHits();
        SearchHit[] rHits = rightHits.getHits();
        Arrays.sort(hits, searchHitsComparator);
        Arrays.sort(rHits, searchHitsComparator);
        for (int i = 0; i < hits.length; i++) {
            assertThat("query: " + query, hits[i].getScore(), equalTo(rHits[i].getScore()));
        }
        for (int i = 0; i < hits.length; i++) {
            if (hits[i].getScore() == hits[hits.length-1].getScore()) {
                return; // we need to cut off here since this is the tail of the queue and we might not have fetched enough docs
            }
            assertThat("query: " + query,hits[i].getId(), equalTo(rHits[i].getId()));
        }
    }

    private static void assertEquivalentOrSubstringMatch(String query, SearchResponse plain, SearchResponse rescored) {
        assertNoFailures(plain);
        assertNoFailures(rescored);
        SearchHits leftHits = plain.getHits();
        SearchHits rightHits = rescored.getHits();
        assertThat(leftHits.getTotalHits(), equalTo(rightHits.getTotalHits()));
        assertThat(leftHits.getHits().length, equalTo(rightHits.getHits().length));
        SearchHit[] hits = leftHits.getHits();
        SearchHit[] otherHits = rightHits.getHits();
        if (!hits[0].getId().equals(otherHits[0].getId())) {
            assertThat(((String) otherHits[0].sourceAsMap().get("field1")).contains(query), equalTo(true));
        } else {
            Arrays.sort(hits, searchHitsComparator);
            Arrays.sort(otherHits, searchHitsComparator);
            for (int i = 0; i < hits.length; i++) {
                if (hits[i].getScore() == hits[hits.length-1].getScore()) {
                    return; // we need to cut off here since this is the tail of the queue and we might not have fetched enough docs
                }
                assertThat(query, hits[i].getId(), equalTo(rightHits.getHits()[i].getId()));
            }
        }
    }

    @Test
    // forces QUERY_THEN_FETCH because of https://github.com/elasticsearch/elasticsearch/issues/4829
    public void testEquivalence() throws Exception {
        // no dummy docs since merges can change scores while we run queries.
        int numDocs = indexRandomNumbers("whitespace", -1, false);

        final int iters = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < iters; i++) {
            int resultSize = numDocs;
            int rescoreWindow = between(1, 3) * resultSize;
            String intToEnglish = English.intToEnglish(between(0, numDocs-1));
            String query = intToEnglish.split(" ")[0];
            SearchResponse rescored = client()
                    .prepareSearch()
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setPreference("test") // ensure we hit the same shards for tie-breaking
                    .setQuery(QueryBuilders.matchQuery("field1", query).operator(MatchQueryBuilder.Operator.OR))
                    .setFrom(0)
                    .setSize(resultSize)
                    .setRescorer(
                            RescoreBuilder
                                    .queryRescorer(
                                            QueryBuilders
                                                    .constantScoreQuery(QueryBuilders.matchPhraseQuery("field1", intToEnglish).slop(3)))
                                    .setQueryWeight(1.0f)
                                    .setRescoreQueryWeight(0.0f)) // no weight - so we basically use the same score as the actual query
                    .setRescoreWindow(rescoreWindow).execute().actionGet();

            SearchResponse plain = client().prepareSearch()
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setPreference("test") // ensure we hit the same shards for tie-breaking
                    .setQuery(QueryBuilders.matchQuery("field1", query).operator(MatchQueryBuilder.Operator.OR)).setFrom(0).setSize(resultSize)
                    .execute().actionGet();
            
            // check equivalence
            assertEquivalent(query, plain, rescored);

            rescored = client()
                    .prepareSearch()
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setPreference("test") // ensure we hit the same shards for tie-breaking
                    .setQuery(QueryBuilders.matchQuery("field1", query).operator(MatchQueryBuilder.Operator.OR))
                    .setFrom(0)
                    .setSize(resultSize)
                    .setRescorer(
                            RescoreBuilder
                                    .queryRescorer(
                                            QueryBuilders
                                                    .constantScoreQuery(QueryBuilders.matchPhraseQuery("field1", "not in the index").slop(3)))
                                    .setQueryWeight(1.0f)
                                    .setRescoreQueryWeight(1.0f))
                    .setRescoreWindow(rescoreWindow).execute().actionGet();
            // check equivalence
            assertEquivalent(query, plain, rescored);

            rescored = client()
                    .prepareSearch()
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setPreference("test") // ensure we hit the same shards for tie-breaking
                    .setQuery(QueryBuilders.matchQuery("field1", query).operator(MatchQueryBuilder.Operator.OR))
                    .setFrom(0)
                    .setSize(resultSize)
                    .setRescorer(
                            RescoreBuilder
                                    .queryRescorer(
                                            QueryBuilders.matchPhraseQuery("field1", intToEnglish).slop(0))
                                    .setQueryWeight(1.0f).setRescoreQueryWeight(1.0f)).setRescoreWindow(2 * rescoreWindow).execute().actionGet();
            // check equivalence or if the first match differs we check if the phrase is a substring of the top doc
            assertEquivalentOrSubstringMatch(intToEnglish, plain, rescored);
        }
    }

    @Test
    public void testExplain() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("analyzer", "whitespace").field("type", "string").endObject().endObject().endObject().endObject())
        );
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "the quick lazy huge brown fox jumps over the tree").execute()
                .actionGet();
        client().prepareIndex("test", "type1", "3")
                .setSource("field1", "quick huge brown", "field2", "the quick lazy huge brown fox jumps over the tree").execute()
                .actionGet();
        refresh();

        {
            SearchResponse searchResponse = client()
                    .prepareSearch()
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(MatchQueryBuilder.Operator.OR))
                    .setRescorer(
                            RescoreBuilder.queryRescorer(QueryBuilders.matchPhraseQuery("field1", "the quick brown").slop(2).boost(4.0f))
                                    .setQueryWeight(0.5f).setRescoreQueryWeight(0.4f)).setRescoreWindow(5).setExplain(true).execute()
                    .actionGet();
            assertHitCount(searchResponse, 3);
            assertFirstHit(searchResponse, hasId("1"));
            assertSecondHit(searchResponse, hasId("2"));
            assertThirdHit(searchResponse, hasId("3"));

            for (int i = 0; i < 3; i++) {
                assertThat(searchResponse.getHits().getAt(i).explanation(), notNullValue());
                assertThat(searchResponse.getHits().getAt(i).explanation().isMatch(), equalTo(true));
                assertThat(searchResponse.getHits().getAt(i).explanation().getDetails().length, equalTo(2));
                assertThat(searchResponse.getHits().getAt(i).explanation().getDetails()[0].isMatch(), equalTo(true));
                if (i == 2) {
                    assertThat(searchResponse.getHits().getAt(i).explanation().getDetails()[1].getValue(), equalTo(0.5f));
                } else {
                    assertThat(searchResponse.getHits().getAt(i).explanation().getDescription(), equalTo("sum of:"));
                    assertThat(searchResponse.getHits().getAt(i).explanation().getDetails()[0].getDetails()[1].getValue(), equalTo(0.5f));
                    assertThat(searchResponse.getHits().getAt(i).explanation().getDetails()[1].getDetails()[1].getValue(), equalTo(0.4f));
                }
            }
        }

        String[] scoreModes = new String[]{ "max", "min", "avg", "total", "multiply", "" };
        String[] descriptionModes = new String[]{ "max of:", "min of:", "avg of:", "sum of:", "product of:", "sum of:" };
        for (int innerMode = 0; innerMode < scoreModes.length; innerMode++) {
            QueryRescorer innerRescoreQuery = RescoreBuilder.queryRescorer(QueryBuilders.matchQuery("field1", "the quick brown").boost(4.0f))
                .setQueryWeight(0.5f).setRescoreQueryWeight(0.4f);

            if (!"".equals(scoreModes[innerMode])) {
                innerRescoreQuery.setScoreMode(scoreModes[innerMode]);
            }

            SearchResponse searchResponse = client()
                    .prepareSearch()
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(MatchQueryBuilder.Operator.OR))
                    .setRescorer(innerRescoreQuery).setRescoreWindow(5).setExplain(true).execute()
                    .actionGet();
            assertHitCount(searchResponse, 3);
            assertFirstHit(searchResponse, hasId("1"));
            assertSecondHit(searchResponse, hasId("2"));
            assertThirdHit(searchResponse, hasId("3"));

            for (int j = 0; j < 3; j++) {
                assertThat(searchResponse.getHits().getAt(j).explanation().getDescription(), equalTo(descriptionModes[innerMode]));
            }

            for (int outerMode = 0; outerMode < scoreModes.length; outerMode++) {
                QueryRescorer outerRescoreQuery = RescoreBuilder.queryRescorer(QueryBuilders.matchQuery("field1", "the quick brown")
                        .boost(4.0f)).setQueryWeight(0.5f).setRescoreQueryWeight(0.4f);

                if (!"".equals(scoreModes[outerMode])) {
                    outerRescoreQuery.setScoreMode(scoreModes[outerMode]);
                }

                searchResponse = client()
                        .prepareSearch()
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(MatchQueryBuilder.Operator.OR))
                        .addRescorer(innerRescoreQuery).setRescoreWindow(5)
                        .addRescorer(outerRescoreQuery).setRescoreWindow(10)
                        .setExplain(true).get();
                assertHitCount(searchResponse, 3);
                assertFirstHit(searchResponse, hasId("1"));
                assertSecondHit(searchResponse, hasId("2"));
                assertThirdHit(searchResponse, hasId("3"));

                for (int j = 0; j < 3; j++) {
                    Explanation explanation = searchResponse.getHits().getAt(j).explanation();
                    assertThat(explanation.getDescription(), equalTo(descriptionModes[outerMode]));
                    assertThat(explanation.getDetails()[0].getDetails()[0].getDescription(), equalTo(descriptionModes[innerMode]));
                }
            }
        }
    }

    @Test
    public void testScoring() throws Exception {
        int numDocs = indexRandomNumbers("keyword");

        String[] scoreModes = new String[]{ "max", "min", "avg", "total", "multiply", "" };
        float primaryWeight = 1.1f;
        float secondaryWeight = 1.6f;

        for (String scoreMode: scoreModes) {
            for (int i = 0; i < numDocs - 4; i++) {
                String[] intToEnglish = new String[] { English.intToEnglish(i), English.intToEnglish(i + 1), English.intToEnglish(i + 2), English.intToEnglish(i + 3) };

                QueryRescorer rescoreQuery = RescoreBuilder
                        .queryRescorer(
                                QueryBuilders.boolQuery()
                                        .disableCoord(true)
                                        .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[0])).boostMode(CombineFunction.REPLACE).add(ScoreFunctionBuilders.scriptFunction("5.0f")))
                                    .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[1])).boostMode(CombineFunction.REPLACE).add(ScoreFunctionBuilders.scriptFunction("7.0f")))
                                    .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[3])).boostMode(CombineFunction.REPLACE).add(ScoreFunctionBuilders.scriptFunction("0.0f"))))
                        .setQueryWeight(primaryWeight)
                        .setRescoreQueryWeight(secondaryWeight);

                if (!"".equals(scoreMode)) {
                    rescoreQuery.setScoreMode(scoreMode);
                }

                SearchResponse rescored = client()
                        .prepareSearch()
                        .setPreference("test") // ensure we hit the same shards for tie-breaking
                        .setQuery(QueryBuilders.boolQuery()
                                .disableCoord(true)
                                .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[0])).boostMode(CombineFunction.REPLACE).add(ScoreFunctionBuilders.scriptFunction("2.0f")))
                                .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[1])).boostMode(CombineFunction.REPLACE).add(ScoreFunctionBuilders.scriptFunction("3.0f")))
                                .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[2])).boostMode(CombineFunction.REPLACE).add(ScoreFunctionBuilders.scriptFunction("5.0f")))
                                .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[3])).boostMode(CombineFunction.REPLACE).add(ScoreFunctionBuilders.scriptFunction("0.2f"))))
                        .setFrom(0)
                        .setSize(10)
                        .setRescorer(rescoreQuery)
                        .setRescoreWindow(50).execute().actionGet();

                assertHitCount(rescored, 4);

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

    @Test
    public void testMultipleRescores() throws Exception {
        int numDocs = indexRandomNumbers("keyword", 1, true);
        QueryRescorer eightIsGreat = RescoreBuilder.queryRescorer(
                QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", English.intToEnglish(8))).boostMode(CombineFunction.REPLACE)
                .add(ScoreFunctionBuilders.scriptFunction("1000.0f"))).setScoreMode("total");
        QueryRescorer sevenIsBetter = RescoreBuilder.queryRescorer(
                QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", English.intToEnglish(7))).boostMode(CombineFunction.REPLACE)
                .add(ScoreFunctionBuilders.scriptFunction("10000.0f"))).setScoreMode("total");

        // First set the rescore window large enough that both rescores take effect
        SearchRequestBuilder request = client().prepareSearch().setRescoreWindow(numDocs);
        request.addRescorer(eightIsGreat).addRescorer(sevenIsBetter);
        SearchResponse response = request.get();
        assertFirstHit(response, hasId("7"));
        assertSecondHit(response, hasId("8"));

        // Now squash the second rescore window so it never gets to see a seven
        response = request.setSize(1).clearRescorers().addRescorer(eightIsGreat).addRescorer(sevenIsBetter, 1).get();
        assertFirstHit(response, hasId("8"));
        // We have no idea what the second hit will be because we didn't get a chance to look for seven

        // Now use one rescore to drag the number we're looking for into the window of another
        QueryRescorer ninetyIsGood = RescoreBuilder.queryRescorer(
                QueryBuilders.functionScoreQuery(QueryBuilders.queryString("*ninety*")).boostMode(CombineFunction.REPLACE)
                .add(ScoreFunctionBuilders.scriptFunction("1000.0f"))).setScoreMode("total");
        QueryRescorer oneToo = RescoreBuilder.queryRescorer(
                QueryBuilders.functionScoreQuery(QueryBuilders.queryString("*one*")).boostMode(CombineFunction.REPLACE)
                .add(ScoreFunctionBuilders.scriptFunction("1000.0f"))).setScoreMode("total");
        request.clearRescorers().addRescorer(ninetyIsGood).addRescorer(oneToo, 10);
        response = request.setSize(2).get();
        assertFirstHit(response, hasId("91"));
        assertFirstHit(response, hasScore(2001.0f));
        assertSecondHit(response, hasScore(1001.0f)); // Not sure which one it is but it is ninety something
    }

    private int indexRandomNumbers(String analyzer) throws Exception {
        return indexRandomNumbers(analyzer, -1, true);
    }

    private int indexRandomNumbers(String analyzer, int shards, boolean dummyDocs) throws Exception {
        Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());

        if (shards > 0) {
            builder.put(SETTING_NUMBER_OF_SHARDS, shards);
        }

        assertAcked(prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("analyzer", analyzer).field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(builder));
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }

        indexRandom(true, dummyDocs, docs);
        ensureGreen();
        return numDocs;
    }
}
