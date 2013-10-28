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

package org.elasticsearch.search.rescore;



import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.rescore.RescoreBuilder.QueryRescorer;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class QueryRescorerTests extends AbstractIntegrationTest {

    @Test
    public void testRescorePhrase() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("analyzer", "whitespace").field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2)).execute().actionGet();

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

        client().admin().indices().prepareCreate("test").addMapping("type1", mapping).setSettings(builder.put("index.number_of_shards", 1))
                .execute().actionGet();

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

    private static final void assertEquivalent(SearchResponse plain, SearchResponse rescored) {
        assertNoFailures(plain);
        assertNoFailures(rescored);
        SearchHits leftHits = plain.getHits();
        SearchHits rightHits = rescored.getHits();
        assertThat(leftHits.getTotalHits(), equalTo(rightHits.getTotalHits()));
        assertThat(leftHits.getHits().length, equalTo(rightHits.getHits().length));
        SearchHit[] hits = leftHits.getHits();
        for (int i = 0; i < hits.length; i++) {
            assertThat(hits[i].getId(), equalTo(rightHits.getHits()[i].getId()));
        }
    }

    private static final void assertEquivalentOrSubstringMatch(String query, SearchResponse plain, SearchResponse rescored) {
        SearchHits leftHits = plain.getHits();
        SearchHits rightHits = rescored.getHits();
        assertThat(leftHits.getTotalHits(), equalTo(rightHits.getTotalHits()));
        assertThat(leftHits.getHits().length, equalTo(rightHits.getHits().length));
        SearchHit[] hits = leftHits.getHits();
        SearchHit[] otherHits = rightHits.getHits();
        if (!hits[0].getId().equals(otherHits[0].getId())) {
            assertThat(((String) otherHits[0].sourceAsMap().get("field1")).contains(query), equalTo(true));
        } else {
            for (int i = 0; i < hits.length; i++) {
                assertThat(query, hits[i].getId(), equalTo(rightHits.getHits()[i].getId()));
            }
        }
    }

    @Test
    public void testEquivalence() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("analyzer", "whitespace").field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder()).execute().actionGet();
        ensureGreen();
        int numDocs = atLeast(100);

        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i)).execute().actionGet();
        }

        flush();
        optimize(); // make sure we don't have a background merge running
        refresh();
        ensureGreen();
        final int iters = atLeast(50);
        for (int i = 0; i < iters; i++) {
            String intToEnglish = English.intToEnglish(between(0, numDocs-1));
            String query = intToEnglish.split(" ")[0];
            SearchResponse rescored = client()
                    .prepareSearch()
                    .setPreference("test") // ensure we hit the same shards for tie-breaking
                    .setQuery(QueryBuilders.matchQuery("field1", query).operator(MatchQueryBuilder.Operator.OR))
                    .setFrom(0)
                    .setSize(10)
                    .setRescorer(
                            RescoreBuilder
                                    .queryRescorer(
                                            QueryBuilders
                                                    .constantScoreQuery(QueryBuilders.matchPhraseQuery("field1", intToEnglish).slop(3)))
                                    .setQueryWeight(1.0f)
                                    .setRescoreQueryWeight(0.0f)) // no weigth - so we basically use the same score as the actual query
                    .setRescoreWindow(50).execute().actionGet();

            SearchResponse plain = client().prepareSearch()
                    .setPreference("test") // ensure we hit the same shards for tie-breaking
                    .setQuery(QueryBuilders.matchQuery("field1", query).operator(MatchQueryBuilder.Operator.OR)).setFrom(0).setSize(10)
                    .execute().actionGet();
            
            // check equivalence
            assertEquivalent(plain, rescored);

            rescored = client()
                    .prepareSearch()
                    .setPreference("test") // ensure we hit the same shards for tie-breaking
                    .setQuery(QueryBuilders.matchQuery("field1", query).operator(MatchQueryBuilder.Operator.OR))
                    .setFrom(0)
                    .setSize(10)
                    .setRescorer(
                            RescoreBuilder
                                    .queryRescorer(
                                            QueryBuilders
                                                    .constantScoreQuery(QueryBuilders.matchPhraseQuery("field1", "not in the index").slop(3)))
                                    .setQueryWeight(1.0f)
                                    .setRescoreQueryWeight(1.0f))
                    .setRescoreWindow(50).execute().actionGet();
            // check equivalence
            assertEquivalent(plain, rescored);

            rescored = client()
                    .prepareSearch()
                    .setPreference("test") // ensure we hit the same shards for tie-breaking
                    .setQuery(QueryBuilders.matchQuery("field1", query).operator(MatchQueryBuilder.Operator.OR))
                    .setFrom(0)
                    .setSize(10)
                    .setRescorer(
                            RescoreBuilder
                                    .queryRescorer(
                                            QueryBuilders.matchPhraseQuery("field1", intToEnglish).slop(0))
                                    .setQueryWeight(1.0f).setRescoreQueryWeight(1.0f)).setRescoreWindow(100).execute().actionGet();
            // check equivalence or if the first match differs we check if the phrase is a substring of the top doc
            assertEquivalentOrSubstringMatch(intToEnglish, plain, rescored);
        }
    }

    @Test
    public void testExplain() throws Exception {
        prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("analyzer", "whitespace").field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2)).execute().actionGet();
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
        for (int i = 0; i < scoreModes.length; i++) {
            QueryRescorer rescoreQuery = RescoreBuilder.queryRescorer(QueryBuilders.matchQuery("field1", "the quick brown").boost(4.0f))
                .setQueryWeight(0.5f).setRescoreQueryWeight(0.4f);

            if (!"".equals(scoreModes[i])) {
                rescoreQuery.setScoreMode(scoreModes[i]);
            }

            SearchResponse searchResponse = client()
                    .prepareSearch()
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.matchQuery("field1", "the quick brown").operator(MatchQueryBuilder.Operator.OR))
                    .setRescorer(rescoreQuery).setRescoreWindow(5).setExplain(true).execute()
                    .actionGet();
            assertHitCount(searchResponse, 3);
            assertFirstHit(searchResponse, hasId("1"));
            assertSecondHit(searchResponse, hasId("2"));
            assertThirdHit(searchResponse, hasId("3"));

            for (int j = 0; j < 3; j++) {
                assertThat(searchResponse.getHits().getAt(j).explanation().getDescription(), equalTo(descriptionModes[i]));
            }
        }
    }

    @Test
    public void testScoring() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("index", "not_analyzed").field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder()).execute().actionGet();
        ensureGreen();
        int numDocs = 1000;

        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i)).execute().actionGet();
        }

        flush();
        optimize(); // make sure we don't have a background merge running
        refresh();
        ensureGreen();

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
                                    .should(QueryBuilders.customScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[0])).script("5.0f"))
                                    .should(QueryBuilders.customScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[1])).script("7.0f"))
                                    .should(QueryBuilders.customScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[3])).script("0.0f")))
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
                                .should(QueryBuilders.customScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[0])).script("2.0f"))
                                .should(QueryBuilders.customScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[1])).script("3.0f"))
                                .should(QueryBuilders.customScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[2])).script("5.0f"))
                                .should(QueryBuilders.customScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[3])).script("0.2f")))
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
    @Slow
    public void testScoring_withFunctionScore() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("index", "not_analyzed").field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder()).execute().actionGet();
        ensureGreen();
        int numDocs = 1000;

        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i)).execute().actionGet();
        }

        flush();
        optimize(); // make sure we don't have a background merge running
        refresh();
        ensureGreen();

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
                                    .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[0]), scriptFunction("5.0f")).boostMode(CombineFunction.REPLACE.getName()))
                                    .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[1]), scriptFunction("7.0f")).boostMode(CombineFunction.REPLACE.getName()))
                                    .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[3]), scriptFunction("0.0f")).boostMode(CombineFunction.REPLACE.getName())))
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
                                .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[0]), scriptFunction("2.0f")).boostMode(CombineFunction.REPLACE.getName()))
                                .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[1]), scriptFunction("3.0f")).boostMode(CombineFunction.REPLACE.getName()))
                                .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[2]), scriptFunction("5.0f")).boostMode(CombineFunction.REPLACE.getName()))
                                .should(QueryBuilders.functionScoreQuery(QueryBuilders.termQuery("field1", intToEnglish[3]), scriptFunction("0.2f")).boostMode(CombineFunction.REPLACE.getName())))
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

}
