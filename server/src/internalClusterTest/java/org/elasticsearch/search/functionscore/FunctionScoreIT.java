/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.functionscore;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

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

public class FunctionScoreIT extends ESIntegTestCase {

    static final String TYPE = "type";
    static final String INDEX = "index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("1", vars -> 1.0d);
            scripts.put("get score value", vars -> ((Number) vars.get("_score")).doubleValue());
            scripts.put("return (doc['num'].value)", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Longs num = (ScriptDocValues.Longs) doc.get("num");
                return num.getValue();
            });
            scripts.put("doc['random_score']", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Doubles randomScore = (ScriptDocValues.Doubles) doc.get("random_score");
                return randomScore.getValue();
            });
            return scripts;
        }
    }

    public void testScriptScoresNested() throws IOException {
        createIndex(INDEX);
        index(INDEX, "1", jsonBuilder().startObject().field("dummy_field", 1).endObject());
        refresh();

        Script scriptOne = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "1", Collections.emptyMap());
        Script scriptTwo = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "get score value", Collections.emptyMap());

        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(
                                        functionScoreQuery(
                                                functionScoreQuery(scriptFunction(scriptOne)),
                                                scriptFunction(scriptTwo)),
                                                scriptFunction(scriptTwo)
                                )
                        )
                )
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).getScore(), equalTo(1.0f));
    }

    public void testScriptScoresWithAgg() throws IOException {
        createIndex(INDEX);
        index(INDEX, "1", jsonBuilder().startObject().field("dummy_field", 1).endObject());
        refresh();

        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "get score value", Collections.emptyMap());

        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource()
                                .query(functionScoreQuery(scriptFunction(script)))
                                .aggregation(terms("score_agg").script(script))
                )
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).getScore(), equalTo(1.0f));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getKeyAsString(), equalTo("1.0"));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getDocCount(), is(1L));
    }

    public void testMinScoreFunctionScoreBasic() throws IOException {
        float score = randomValueOtherThanMany((f) -> Float.compare(f, 0) < 0, ESTestCase::randomFloat);
        float minScore = randomValueOtherThanMany((f) -> Float.compare(f, 0) < 0, ESTestCase::randomFloat);
        index(INDEX, jsonBuilder().startObject()
            .field("num", 2)
            .field("random_score", score) // Pass the random score as a document field so that it can be extracted in the script
            .endObject());
        refresh();
        ensureYellow();

        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['random_score']", Collections.emptyMap());
        SearchResponse searchResponse = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery(scriptFunction(script)).setMinScore(minScore)))
        ).actionGet();
        if (score < minScore) {
            assertThat(searchResponse.getHits().getTotalHits().value, is(0L));
        } else {
            assertThat(searchResponse.getHits().getTotalHits().value, is(1L));
        }

        searchResponse = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery(new MatchAllQueryBuilder(), new FilterFunctionBuilder[] {
                                new FilterFunctionBuilder(scriptFunction(script)),
                                new FilterFunctionBuilder(scriptFunction(script))
                        }).scoreMode(FunctionScoreQuery.ScoreMode.AVG).setMinScore(minScore)))
                ).actionGet();
        if (score < minScore) {
            assertThat(searchResponse.getHits().getTotalHits().value, is(0L));
        } else {
            assertThat(searchResponse.getHits().getTotalHits().value, is(1L));
        }
    }

    public void testMinScoreFunctionScoreManyDocsAndRandomMinScore() throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int numDocs = randomIntBetween(1, 100);
        int scoreOffset = randomIntBetween(0, 2 * numDocs);
        int minScore = randomIntBetween(0, 2 * numDocs);
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("num", i + scoreOffset));
        }
        indexRandom(true, docs);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "return (doc['num'].value)", Collections.emptyMap());
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
                }).scoreMode(FunctionScoreQuery.ScoreMode.AVG).setMinScore(minScore)).size(numDocs))).actionGet();
        assertMinScoreSearchResponses(numDocs, searchResponse, numMatchingDocs);
    }

    protected void assertMinScoreSearchResponses(int numDocs, SearchResponse searchResponse, int numMatchingDocs) {
        assertSearchResponse(searchResponse);
        assertThat((int) searchResponse.getHits().getTotalHits().value, is(numMatchingDocs));
        int pos = 0;
        for (int hitId = numDocs - 1; (numDocs - hitId) < searchResponse.getHits().getTotalHits().value; hitId--) {
            assertThat(searchResponse.getHits().getAt(pos).getId(), equalTo(Integer.toString(hitId)));
            pos++;
        }
    }

    /** make sure min_score works if functions is empty, see https://github.com/elastic/elasticsearch/issues/10253 */
    public void testWithEmptyFunctions() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test"));
        index("test", "1", jsonBuilder().startObject().field("text", "test text").endObject());
        refresh();

        SearchResponse termQuery = client().search(
            searchRequest().source(
                    searchSource().explain(true).query(
                            termQuery("text", "text")))).get();
        assertSearchResponse(termQuery);
        assertThat(termQuery.getHits().getTotalHits().value, equalTo(1L));
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
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
        assertThat(response.getHits().getAt(0).getScore(), equalTo(expectedScore));

        response = client().search(
                searchRequest().source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("text", "text")).boostMode(boostMode).setMinScore(2f)))).get();

        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, equalTo(0L));
    }
}

