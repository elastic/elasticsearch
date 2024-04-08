/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.functionscore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.DocValuesDocReader;
import org.elasticsearch.script.ExplainableScoreScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class ExplainableScriptIT extends ESIntegTestCase {

    public static class ExplainableScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "test";
                }

                @Override
                public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context, Map<String, String> params) {
                    assert scriptSource.equals("explainable_script");
                    assert context == ScoreScript.CONTEXT;
                    ScoreScript.Factory factory = (params1, lookup) -> new ScoreScript.LeafFactory() {
                        @Override
                        public boolean needs_score() {
                            return false;
                        }

                        @Override
                        public ScoreScript newInstance(DocReader docReader) {
                            return new MyScript(params1, lookup, ((DocValuesDocReader) docReader).getLeafReaderContext());
                        }
                    };
                    return context.factoryClazz.cast(factory);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(ScoreScript.CONTEXT);
                }
            };
        }
    }

    static class MyScript extends ScoreScript implements ExplainableScoreScript {

        MyScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
            super(params, null, new DocValuesDocReader(lookup, leafContext));
        }

        @Override
        public Explanation explain(Explanation subQueryScore) throws IOException {
            double score = execute(null);
            return Explanation.match((float) score, "This script returned " + score);
        }

        @Override
        public double execute(ExplanationHolder explanation) {
            return ((Number) (getDoc().get("number_field")).get(0)).doubleValue();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ExplainableScriptPlugin.class);
    }

    public void testExplainScript() throws InterruptedException, IOException, ExecutionException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            indexRequests.add(
                prepareIndex("test").setId(Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("number_field", i).field("text", "text").endObject())
            );
        }
        indexRandom(true, true, indexRequests);
        client().admin().indices().prepareRefresh().get();
        ensureYellow();
        assertNoFailuresAndResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().explain(true)
                            .query(
                                functionScoreQuery(
                                    termQuery("text", "text"),
                                    scriptFunction(new Script(ScriptType.INLINE, "test", "explainable_script", Collections.emptyMap()))
                                ).boostMode(CombineFunction.REPLACE)
                            )
                    )
            ),
            response -> {
                SearchHits hits = response.getHits();
                assertThat(hits.getTotalHits().value, equalTo(20L));
                int idCounter = 19;
                for (SearchHit hit : hits.getHits()) {
                    assertThat(hit.getId(), equalTo(Integer.toString(idCounter)));
                    assertThat(hit.getExplanation().toString(), containsString(Double.toString(idCounter)));
                    assertThat(hit.getExplanation().getDetails().length, equalTo(2));
                    idCounter--;
                }
            }
        );
    }
}
