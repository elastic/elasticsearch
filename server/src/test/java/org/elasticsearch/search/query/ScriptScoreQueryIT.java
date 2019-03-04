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

package org.elasticsearch.search.query;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.scriptScoreQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThirdHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasScore;

public class ScriptScoreQueryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("doc['field2'].value * param1", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Doubles field2Values = (ScriptDocValues.Doubles) doc.get("field2");
                Double param1 = (Double) vars.get("param1");
                return field2Values.getValue() * param1;
            });
            return scripts;
        }
    }

    // test that script_score works as expected:
    // 1) only matched docs retrieved
    // 2) score is calculated based on a script with params
    // 3) min score applied
    public void testScriptScore() {
        assertAcked(
            prepareCreate("test-index").addMapping("_doc", "field1", "type=text", "field2", "type=double")
        );
        int docCount = 10;
        for (int i = 1; i <= docCount; i++) {
            client().prepareIndex("test-index", "_doc", "" + i)
                .setSource("field1", "text" + (i % 2), "field2", i )
                .get();
        }
        refresh();

        Map<String, Object> params = new HashMap<>();
        params.put("param1", 0.1);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
        SearchResponse resp = client()
            .prepareSearch("test-index")
            .setQuery(scriptScoreQuery(matchQuery("field1", "text0"), new ScriptScoreFunctionBuilder(script)))
            .get();
        assertNoFailures(resp);
        assertOrderedSearchHits(resp, "10", "8", "6", "4", "2");
        assertFirstHit(resp, hasScore(1.0f));
        assertSecondHit(resp, hasScore(0.8f));
        assertThirdHit(resp, hasScore(0.6f));

        // applying min score
        resp = client()
            .prepareSearch("test-index")
            .setQuery(scriptScoreQuery(matchQuery("field1", "text0"), new ScriptScoreFunctionBuilder(script)).setMinScore(0.6f))
            .get();
        assertNoFailures(resp);
        assertOrderedSearchHits(resp, "10", "8", "6");
    }
}
