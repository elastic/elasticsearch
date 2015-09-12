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

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.AbstractDoubleSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ExplainableSearchScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class ExplainableScriptIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(ExplainableScriptPlugin.class);
    }

    @Test
    public void testNativeExplainScript() throws InterruptedException, IOException, ExecutionException {

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
                                functionScoreQuery(termQuery("text", "text")).add(
                                        scriptFunction(new Script("native_explainable_script", ScriptType.INLINE, "native", null)))
                                        .boostMode("replace")))).actionGet();

        ElasticsearchAssertions.assertNoFailures(response);
        SearchHits hits = response.getHits();
        assertThat(hits.getTotalHits(), equalTo(20l));
        int idCounter = 19;
        for (SearchHit hit : hits.getHits()) {
            assertThat(hit.getId(), equalTo(Integer.toString(idCounter)));
            assertThat(hit.explanation().toString(), containsString(Double.toString(idCounter) + " = This script returned " + Double.toString(idCounter)));
            assertThat(hit.explanation().toString(), containsString("1.0 = tf(freq=1.0), with freq of"));
            assertThat(hit.explanation().getDetails().length, equalTo(2));
            idCounter--;
        }
    }

    public static class MyNativeScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new MyScript();
        }
        @Override
        public boolean needsScores() {
            return true;
        }
    }

    static class MyScript extends AbstractDoubleSearchScript implements ExplainableSearchScript, ExecutableScript {

        @Override
        public Explanation explain(Explanation subQueryScore) throws IOException {
            Explanation scoreExp = Explanation.match(subQueryScore.getValue(), "_score: ", subQueryScore);
            return Explanation.match((float) (runAsDouble()), "This script returned " + runAsDouble(), scoreExp);
        }

        @Override
        public double runAsDouble() {
            return ((Number) ((ScriptDocValues) doc().get("number_field")).getValues().get(0)).doubleValue();
        }
    }
}
