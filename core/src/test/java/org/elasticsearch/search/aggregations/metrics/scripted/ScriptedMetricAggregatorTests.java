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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptMetrics;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.singleton;

public class ScriptedMetricAggregatorTests extends AggregatorTestCase {

    private static final String AGG_NAME = "scriptedMetric";
    private static final Script INIT_SCRIPT = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "initScript", Collections.emptyMap());
    private static final Script MAP_SCRIPT = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "mapScript", Collections.emptyMap());
    private static final Script COMBINE_SCRIPT = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "combineScript",
            Collections.emptyMap());
    private static final Map<String, Function<Map<String, Object>, Object>> SCRIPTS = new HashMap<>();


    @BeforeClass
    @SuppressWarnings("unchecked")
    public static void initMockScripts() {
        SCRIPTS.put("initScript", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            agg.put("collector", new ArrayList<Integer>());
            return agg;
            });
        SCRIPTS.put("mapScript", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            ((List<Integer>) agg.get("collector")).add(1); // just add 1 for each doc the script is run on
            return agg;
        });
        SCRIPTS.put("combineScript", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            return ((List<Integer>) agg.get("collector")).stream().mapToInt(Integer::intValue).sum();
        });
    }

    @SuppressWarnings("unchecked")
    public void testNoDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // intentionally not writing any docs
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.mapScript(MAP_SCRIPT); // map script is mandatory, even if its not used in this case
                ScriptedMetric scriptedMetric = search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder);
                assertEquals(AGG_NAME, scriptedMetric.getName());
                assertNotNull(scriptedMetric.aggregation());
                assertEquals(0, ((HashMap<Object, String>) scriptedMetric.aggregation()).size());
            }
        }
    }

    /**
     * without combine script, the "_aggs" map should contain a list of the size of the number of documents matched
     */
    @SuppressWarnings("unchecked")
    public void testScriptedMetricWithoutCombine() throws IOException {
        try (Directory directory = newDirectory()) {
            int numDocs = randomInt(100);
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numDocs; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT);
                ScriptedMetric scriptedMetric = search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder);
                assertEquals(AGG_NAME, scriptedMetric.getName());
                assertNotNull(scriptedMetric.aggregation());
                Map<String, Object> agg = (Map<String, Object>) scriptedMetric.aggregation();
                assertEquals(numDocs, ((List<Integer>) agg.get("collector")).size());
            }
        }
    }

    /**
     * test that combine script sums the list produced by the "mapScript"
     */
    public void testScriptedMetricWithCombine() throws IOException {
        try (Directory directory = newDirectory()) {
            Integer numDocs = randomInt(100);
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numDocs; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT).combineScript(COMBINE_SCRIPT);
                ScriptedMetric scriptedMetric = search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder);
                assertEquals(AGG_NAME, scriptedMetric.getName());
                assertNotNull(scriptedMetric.aggregation());
                assertEquals(numDocs, scriptedMetric.aggregation());
            }
        }
    }

    /**
     * We cannot use Mockito for mocking QueryShardContext in this case because
     * script-related methods (e.g. QueryShardContext#getLazyExecutableScript)
     * is final and cannot be mocked
     */
    @Override
    protected QueryShardContext queryShardContextMock(final MappedFieldType[] fieldTypes, IndexSettings idxSettings,
            CircuitBreakerService circuitBreakerService) {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                 // no file watching, so we don't need a ResourceWatcherService
                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), "false").build();
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, SCRIPTS);
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singletonList(scriptEngine));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Collections.emptyList());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, null, scriptContextRegistry);
        ScriptService scriptService;
        try {
            scriptService = new ScriptService(settings, new Environment(settings), null, scriptEngineRegistry, scriptContextRegistry,
                    scriptSettings, new ScriptMetrics());
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
        return new QueryShardContext(0, idxSettings, null, null, null, null, scriptService, null, xContentRegistry(),
                null, null, System::currentTimeMillis);
    }
}
