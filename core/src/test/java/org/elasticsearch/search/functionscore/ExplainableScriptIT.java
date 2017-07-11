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

package org.elasticsearch.search.functionscore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ExplainableSearchScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
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
                    assert context == SearchScript.CONTEXT;
                    SearchScript.Factory factory = (p, lookup) -> new SearchScript.LeafFactory() {
                        @Override
                        public SearchScript newInstance(LeafReaderContext context) throws IOException {
                            return new MyScript(lookup.doc().getLeafDocLookup(context));
                        }
                        @Override
                        public boolean needs_score() {
                            return false;
                        }
                    };
                    return context.factoryClazz.cast(factory);
                }
            };
        }
    }

    static class MyScript extends SearchScript implements ExplainableSearchScript {
        LeafDocLookup docLookup;

        MyScript(LeafDocLookup docLookup) {
            super(null, null, null);
            this.docLookup = docLookup;
        }

        @Override
        public void setDocument(int doc) {
            docLookup.setDocument(doc);
        }

        @Override
        public Explanation explain(Explanation subQueryScore) throws IOException {
            Explanation scoreExp = Explanation.match(subQueryScore.getValue(), "_score: ", subQueryScore);
            return Explanation.match((float) (runAsDouble()), "This script returned " + runAsDouble(), scoreExp);
        }

        @Override
        public double runAsDouble() {
            return ((Number) ((ScriptDocValues) docLookup.get("number_field")).getValues().get(0)).doubleValue();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ExplainableScriptPlugin.class);
    }

    public void testExplainScript() throws InterruptedException, IOException, ExecutionException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            indexRequests.add(client().prepareIndex("test", "type").setId(Integer.toString(i)).setSource(
                    jsonBuilder().startObject().field("number_field", i).field("text", "text").endObject()));
        }
        indexRandom(true, true, indexRequests);
        client().admin().indices().prepareRefresh().execute().actionGet();
        ensureYellow();
        SearchResponse response = client().search(searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("text", "text"),
                                        scriptFunction(
                                            new Script(ScriptType.INLINE, "test", "explainable_script", Collections.emptyMap())))
                                        .boostMode(CombineFunction.REPLACE)))).actionGet();

        ElasticsearchAssertions.assertNoFailures(response);
        SearchHits hits = response.getHits();
        assertThat(hits.getTotalHits(), equalTo(20L));
        int idCounter = 19;
        for (SearchHit hit : hits.getHits()) {
            assertThat(hit.getId(), equalTo(Integer.toString(idCounter)));
            assertThat(hit.getExplanation().toString(),
                    containsString(Double.toString(idCounter) + " = This script returned " + Double.toString(idCounter)));
            assertThat(hit.getExplanation().toString(), containsString("freq=1.0"));
            assertThat(hit.getExplanation().toString(), containsString("termFreq=1.0"));
            assertThat(hit.getExplanation().getDetails().length, equalTo(2));
            idCounter--;
        }
    }
}
