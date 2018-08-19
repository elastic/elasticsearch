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

package org.elasticsearch.search.aggregations.metrics.scripted;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * This test verifies that the _agg param is added correctly when the system property
 * "es.aggregations.enable_scripted_metric_agg_param" is set to true.
 */
public class ScriptedMetricAggregatorAggStateV6CompatTests extends AggregatorTestCase {

    private static final String AGG_NAME = "scriptedMetric";
    private static final Script INIT_SCRIPT = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "initScript", Collections.emptyMap());
    private static final Script MAP_SCRIPT = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "mapScript", Collections.emptyMap());
    private static final Script COMBINE_SCRIPT = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "combineScript",
            Collections.emptyMap());

    private static final Script INIT_SCRIPT_EXPLICIT_AGG = new Script(ScriptType.INLINE, MockScriptEngine.NAME,
        "initScriptExplicitAgg", Collections.emptyMap());
    private static final Script MAP_SCRIPT_EXPLICIT_AGG = new Script(ScriptType.INLINE, MockScriptEngine.NAME,
        "mapScriptExplicitAgg", Collections.emptyMap());
    private static final Script COMBINE_SCRIPT_EXPLICIT_AGG = new Script(ScriptType.INLINE, MockScriptEngine.NAME,
        "combineScriptExplicitAgg", Collections.emptyMap());
    private static final String EXPLICIT_AGG_OBJECT = "Explicit agg object";

    private static final Map<String, Function<Map<String, Object>, Object>> SCRIPTS = new HashMap<>();

    @BeforeClass
    @SuppressWarnings("unchecked")
    public static void initMockScripts() {
        // If _agg is provided implicitly, it should be the same objects as "state" from the context.
        SCRIPTS.put("initScript", params -> {
            Object agg = params.get("_agg");
            Object state = params.get("state");
            assertThat(agg, instanceOf(Map.class));
            assertThat(agg, sameInstance(state));
            return agg;
        });
        SCRIPTS.put("mapScript", params -> {
            Object agg = params.get("_agg");
            Object state = params.get("state");
            assertThat(agg, instanceOf(Map.class));
            assertThat(agg, sameInstance(state));
            return agg;
        });
        SCRIPTS.put("combineScript", params -> {
            Object agg = params.get("_agg");
            Object state = params.get("state");
            assertThat(agg, instanceOf(Map.class));
            assertThat(agg, sameInstance(state));
            return agg;
        });

        SCRIPTS.put("initScriptExplicitAgg", params -> {
            Object agg = params.get("_agg");
            assertThat(agg, equalTo(EXPLICIT_AGG_OBJECT));
            return agg;
        });
        SCRIPTS.put("mapScriptExplicitAgg", params -> {
            Object agg = params.get("_agg");
            assertThat(agg, equalTo(EXPLICIT_AGG_OBJECT));
            return agg;
        });
        SCRIPTS.put("combineScriptExplicitAgg", params -> {
            Object agg = params.get("_agg");
            assertThat(agg, equalTo(EXPLICIT_AGG_OBJECT));
            return agg;
        });
    }

    /**
     * Test that the _agg param is implicitly added
     */
    public void testWithImplicitAggParam() throws IOException {
        try (Directory directory = newDirectory()) {
            Integer numDocs = 10;
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numDocs; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT).combineScript(COMBINE_SCRIPT);
                search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder);
            }
        }

        assertWarnings(ScriptedMetricAggContexts.AGG_PARAM_DEPRECATION_WARNING);
    }

    /**
     * Test that an explicitly added _agg param is honored
     */
    public void testWithExplicitAggParam() throws IOException {
        try (Directory directory = newDirectory()) {
            Integer numDocs = 10;
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numDocs; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }

            Map<String, Object> aggParams = new HashMap<>();
            aggParams.put("_agg", EXPLICIT_AGG_OBJECT);

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder
                    .params(aggParams)
                    .initScript(INIT_SCRIPT_EXPLICIT_AGG)
                    .mapScript(MAP_SCRIPT_EXPLICIT_AGG)
                    .combineScript(COMBINE_SCRIPT_EXPLICIT_AGG);
                search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder);
            }
        }

        assertWarnings(ScriptedMetricAggContexts.AGG_PARAM_DEPRECATION_WARNING);
    }

    /**
     * We cannot use Mockito for mocking QueryShardContext in this case because
     * script-related methods (e.g. QueryShardContext#getLazyExecutableScript)
     * is final and cannot be mocked
     */
    @Override
    protected QueryShardContext queryShardContextMock(MapperService mapperService) {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, SCRIPTS);
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);
        ScriptService scriptService =  new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
        return new QueryShardContext(0, mapperService.getIndexSettings(), null, null, mapperService, null, scriptService,
                xContentRegistry(), writableRegistry(), null, null, System::currentTimeMillis, null);
    }
}
