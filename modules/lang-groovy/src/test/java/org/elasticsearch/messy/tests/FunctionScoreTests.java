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

package org.elasticsearch.messy.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FunctionScoreTests extends ESIntegTestCase {
    static final String TYPE = "type";
    static final String INDEX = "index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GroovyPlugin.class);
    }

    public void testScriptScoresNested() throws IOException {
        createIndex(INDEX);
        ensureYellow();
        index(INDEX, TYPE, "1", jsonBuilder().startObject().field("dummy_field", 1).endObject());
        refresh();
        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(
                                        functionScoreQuery(
                                                functionScoreQuery(scriptFunction(new Script("1"))),
                                                scriptFunction(new Script("_score.doubleValue()"))),
                                        scriptFunction(new Script("_score.doubleValue()"))
                                )
                        )
                )
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(1.0f));
    }

    public void testScriptScoresWithAgg() throws IOException {
        createIndex(INDEX);
        ensureYellow();
        index(INDEX, TYPE, "1", jsonBuilder().startObject().field("dummy_field", 1).endObject());
        refresh();
        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().query(functionScoreQuery(scriptFunction(new Script("_score.doubleValue()")))).aggregation(
                                terms("score_agg").script(new Script("_score.doubleValue()")))
                )
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(1.0f));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getKeyAsString(), equalTo("1.0"));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getDocCount(), is(1L));
    }

    public void testMinScoreFunctionScoreBasic() throws IOException {
        index(INDEX, TYPE, jsonBuilder().startObject().field("num", 2).endObject());
        refresh();
        ensureYellow();
        float score = randomFloat();
        float minScore = randomFloat();
        SearchResponse searchResponse = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(scriptFunction(new Script(Float.toString(score)))).setMinScore(minScore)))
        ).actionGet();
        if (score < minScore) {
            assertThat(searchResponse.getHits().getTotalHits(), is(0L));
        } else {
            assertThat(searchResponse.getHits().getTotalHits(), is(1L));
        }

        searchResponse = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery(new MatchAllQueryBuilder(), new FilterFunctionBuilder[] {
                                new FilterFunctionBuilder(scriptFunction(new Script(Float.toString(score)))),
                                new FilterFunctionBuilder(scriptFunction(new Script(Float.toString(score))))
                        }).scoreMode(FiltersFunctionScoreQuery.ScoreMode.AVG).setMinScore(minScore)))
                ).actionGet();
        if (score < minScore) {
            assertThat(searchResponse.getHits().getTotalHits(), is(0L));
        } else {
            assertThat(searchResponse.getHits().getTotalHits(), is(1L));
        }
    }

    public void testMinScoreFunctionScoreManyDocsAndRandomMinScore() throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int numDocs = randomIntBetween(1, 100);
        int scoreOffset = randomIntBetween(-2 * numDocs, 2 * numDocs);
        int minScore = randomIntBetween(-2 * numDocs, 2 * numDocs);
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex(INDEX, TYPE, Integer.toString(i)).setSource("num", i + scoreOffset));
        }
        indexRandom(true, docs);
        ensureYellow();
        Script script = new Script("return (doc['num'].value)");
        int numMatchingDocs = numDocs + scoreOffset - minScore;
        if (numMatchingDocs < 0) {
            numMatchingDocs = 0;
        }
        if (numMatchingDocs > numDocs) {
            numMatchingDocs = numDocs;
        }

        SearchResponse searchResponse = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery(scriptFunction(script))
                        .setMinScore(minScore)).size(numDocs))).actionGet();
        assertMinScoreSearchResponses(numDocs, searchResponse, numMatchingDocs);

        searchResponse = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery(new MatchAllQueryBuilder(), new FilterFunctionBuilder[] {
                        new FilterFunctionBuilder(scriptFunction(script)),
                        new FilterFunctionBuilder(scriptFunction(script))
                }).scoreMode(FiltersFunctionScoreQuery.ScoreMode.AVG).setMinScore(minScore)).size(numDocs))).actionGet();
        assertMinScoreSearchResponses(numDocs, searchResponse, numMatchingDocs);
    }

    protected void assertMinScoreSearchResponses(int numDocs, SearchResponse searchResponse, int numMatchingDocs) {
        assertSearchResponse(searchResponse);
        assertThat((int) searchResponse.getHits().totalHits(), is(numMatchingDocs));
        int pos = 0;
        for (int hitId = numDocs - 1; (numDocs - hitId) < searchResponse.getHits().totalHits(); hitId--) {
            assertThat(searchResponse.getHits().getAt(pos).getId(), equalTo(Integer.toString(hitId)));
            pos++;
        }
    }

    /** make sure min_score works if functions is empty, see https://github.com/elastic/elasticsearch/issues/10253 */
    public void testWithEmptyFunctions() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test"));
        ensureYellow();
        index("test", "testtype", "1", jsonBuilder().startObject().field("text", "test text").endObject());
        refresh();

        SearchResponse termQuery = client().search(
            searchRequest().source(
                    searchSource().explain(true).query(
                            termQuery("text", "text")))).get();
        assertSearchResponse(termQuery);
        assertThat(termQuery.getHits().totalHits(), equalTo(1L));
        float termQueryScore = termQuery.getHits().getAt(0).getScore();

        for (CombineFunction combineFunction : CombineFunction.values()) {
            testMinScoreApplied(combineFunction, termQueryScore);
        }
    }

    protected void testMinScoreApplied(CombineFunction boostMode, float expectedScore) throws InterruptedException, ExecutionException {
        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("text", "text")).boostMode(boostMode).setMinScore(0.1f)))).get();
        assertSearchResponse(response);
        assertThat(response.getHits().totalHits(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getScore(), equalTo(expectedScore));

        response = client().search(
                searchRequest().source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("text", "text")).boostMode(boostMode).setMinScore(2f)))).get();

        assertSearchResponse(response);
        assertThat(response.getHits().totalHits(), equalTo(0L));
    }
}

