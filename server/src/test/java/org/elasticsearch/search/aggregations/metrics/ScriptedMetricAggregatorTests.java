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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
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
    private static final Script REDUCE_SCRIPT = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "reduceScript",
        Collections.emptyMap());

    private static final Script INIT_SCRIPT_SCORE = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "initScriptScore",
            Collections.emptyMap());
    private static final Script MAP_SCRIPT_SCORE = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "mapScriptScore",
            Collections.emptyMap());
    private static final Script COMBINE_SCRIPT_SCORE = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "combineScriptScore",
            Collections.emptyMap());
    private static final Script COMBINE_SCRIPT_NOOP = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "combineScriptNoop",
        Collections.emptyMap());

    private static final Script INIT_SCRIPT_PARAMS = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "initScriptParams",
            Collections.singletonMap("initialValue", 24));
    private static final Script MAP_SCRIPT_PARAMS = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "mapScriptParams",
            Collections.singletonMap("itemValue", 12));
    private static final Script COMBINE_SCRIPT_PARAMS = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "combineScriptParams",
            Collections.singletonMap("multiplier", 4));
    private static final Script REDUCE_SCRIPT_PARAMS = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "reduceScriptParams",
            Collections.singletonMap("additional", 2));
    private static final String CONFLICTING_PARAM_NAME = "initialValue";

    private static final Script INIT_SCRIPT_SELF_REF = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "initScriptSelfRef",
            Collections.emptyMap());
    private static final Script MAP_SCRIPT_SELF_REF = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "mapScriptSelfRef",
            Collections.emptyMap());
    private static final Script COMBINE_SCRIPT_SELF_REF = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "combineScriptSelfRef",
            Collections.emptyMap());

    private static final Map<String, Function<Map<String, Object>, Object>> SCRIPTS = new HashMap<>();

    @BeforeClass
    @SuppressWarnings("unchecked")
    public static void initMockScripts() {
        SCRIPTS.put("initScript", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            state.put("collector", new ArrayList<Integer>());
            return state;
        });
        SCRIPTS.put("mapScript", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            ((List<Integer>) state.get("collector")).add(1); // just add 1 for each doc the script is run on
            return state;
        });
        SCRIPTS.put("combineScript", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            return ((List<Integer>) state.get("collector")).stream().mapToInt(Integer::intValue).sum();
        });
        SCRIPTS.put("combineScriptNoop", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            return state;
        });
        SCRIPTS.put("reduceScript", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            return state;
        });

        SCRIPTS.put("initScriptScore", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            state.put("collector", new ArrayList<Double>());
            return state;
        });
        SCRIPTS.put("mapScriptScore", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            ((List<Double>) state.get("collector")).add(((Number) params.get("_score")).doubleValue());
            return state;
        });
        SCRIPTS.put("combineScriptScore", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            return ((List<Double>) state.get("collector")).stream().mapToDouble(Double::doubleValue).sum();
        });

        SCRIPTS.put("initScriptParams", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            Integer initialValue = (Integer)params.get("initialValue");
            ArrayList<Integer> collector = new ArrayList<>();
            collector.add(initialValue);
            state.put("collector", collector);
            return state;
        });
        SCRIPTS.put("mapScriptParams", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            Integer itemValue = (Integer) params.get("itemValue");
            ((List<Integer>) state.get("collector")).add(itemValue);
            return state;
        });
        SCRIPTS.put("combineScriptParams", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            int multiplier = ((Integer) params.get("multiplier"));
            return ((List<Integer>) state.get("collector")).stream().mapToInt(Integer::intValue).map(i -> i * multiplier).sum();
        });
        SCRIPTS.put("reduceScriptParams", params ->
            ((List)params.get("states")).stream().mapToInt(i -> (int)i).sum() +
                    (int)params.get("aggs_param") + (int)params.get("additional") -
                    ((List)params.get("states")).size()*24*4
        );

        SCRIPTS.put("initScriptSelfRef", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            state.put("collector", new ArrayList<Integer>());
            state.put("selfRef", state);
            return state;
        });

        SCRIPTS.put("mapScriptSelfRef", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            state.put("selfRef", state);
            return state;
        });

        SCRIPTS.put("combineScriptSelfRef", params -> {
           Map<String, Object> state = (Map<String, Object>) params.get("state");
           state.put("selfRef", state);
           return state;
        });
    }

    @Override
    protected ScriptService getMockScriptService() {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME,
            SCRIPTS,
            Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }


    @SuppressWarnings("unchecked")
    public void testNoDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // intentionally not writing any docs
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.mapScript(MAP_SCRIPT).combineScript(COMBINE_SCRIPT_NOOP).reduceScript(REDUCE_SCRIPT);
                ScriptedMetric scriptedMetric = search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder);
                assertEquals(AGG_NAME, scriptedMetric.getName());
                assertNotNull(scriptedMetric.aggregation());
                assertEquals(0, ((HashMap<Object, String>) scriptedMetric.aggregation()).size());
            }
        }
    }

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
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT).reduceScript(REDUCE_SCRIPT);
                IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                    () -> search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder));
                assertEquals(exception.getMessage(), "[combineScript] must not be null: [scriptedMetric]");
            }
        }
    }

    public void testScriptedMetricWithoutReduce() throws IOException {
        try (Directory directory = newDirectory()) {
            int numDocs = randomInt(100);
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numDocs; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT).combineScript(COMBINE_SCRIPT);
                IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                    () -> search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder));
                assertEquals(exception.getMessage(), "[reduceScript] must not be null: [scriptedMetric]");
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
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT)
                    .combineScript(COMBINE_SCRIPT).reduceScript(REDUCE_SCRIPT);
                ScriptedMetric scriptedMetric = search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder);
                assertEquals(AGG_NAME, scriptedMetric.getName());
                assertNotNull(scriptedMetric.aggregation());
                assertEquals(numDocs, scriptedMetric.aggregation());
            }
        }
    }

    /**
     * test that uses the score of the documents
     */
    public void testScriptedMetricWithCombineAccessesScores() throws IOException {
        try (Directory directory = newDirectory()) {
            Integer numDocs = randomInt(100);
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numDocs; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT_SCORE).mapScript(MAP_SCRIPT_SCORE)
                    .combineScript(COMBINE_SCRIPT_SCORE).reduceScript(REDUCE_SCRIPT);
                ScriptedMetric scriptedMetric = search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder);
                assertEquals(AGG_NAME, scriptedMetric.getName());
                assertNotNull(scriptedMetric.aggregation());
                // all documents have score of 1.0
                assertEquals((double) numDocs, scriptedMetric.aggregation());
            }
        }
    }

    public void testScriptParamsPassedThrough() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 100; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT_PARAMS).mapScript(MAP_SCRIPT_PARAMS)
                    .combineScript(COMBINE_SCRIPT_PARAMS).reduceScript(REDUCE_SCRIPT);
                ScriptedMetric scriptedMetric = search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder);

                // The result value depends on the script params.
                assertEquals(4896, scriptedMetric.aggregation());
            }
        }
    }

    public void testAggParamsPassedToReduceScript() throws IOException {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, SCRIPTS, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);
        ScriptService scriptService =  new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 100; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.params(Collections.singletonMap("aggs_param", 1))
                        .initScript(INIT_SCRIPT_PARAMS).mapScript(MAP_SCRIPT_PARAMS)
                        .combineScript(COMBINE_SCRIPT_PARAMS).reduceScript(REDUCE_SCRIPT_PARAMS);
                ScriptedMetric scriptedMetric = searchAndReduce(
                        newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder, 0);

                // The result value depends on the script params.
                assertEquals(4803, scriptedMetric.aggregation());
            }
        }
    }

    public void testConflictingAggAndScriptParams() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 100; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                Map<String, Object> aggParams = Collections.singletonMap(CONFLICTING_PARAM_NAME, "blah");
                aggregationBuilder.params(aggParams).initScript(INIT_SCRIPT_PARAMS).mapScript(MAP_SCRIPT_PARAMS)
                    .combineScript(COMBINE_SCRIPT_PARAMS).reduceScript(REDUCE_SCRIPT);

                IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
                    search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder)
                );
                assertEquals("Parameter name \"" + CONFLICTING_PARAM_NAME + "\" used in both aggregation and script parameters",
                    ex.getMessage());
            }
        }
    }

    public void testSelfReferencingAggStateAfterInit() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // No need to add docs for this test
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT_SELF_REF).mapScript(MAP_SCRIPT)
                    .combineScript(COMBINE_SCRIPT_PARAMS).reduceScript(REDUCE_SCRIPT);

                IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
                    search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder)
                );
                assertEquals("Iterable object is self-referencing itself (Scripted metric aggs init script)", ex.getMessage());
            }
        }
    }

    public void testSelfReferencingAggStateAfterMap() throws IOException {
        try (Directory directory = newDirectory()) {
            Integer numDocs = randomIntBetween(1, 100);
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numDocs; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT_SELF_REF)
                    .combineScript(COMBINE_SCRIPT_PARAMS).reduceScript(REDUCE_SCRIPT);

                IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
                    search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder)
                );
                assertEquals("Iterable object is self-referencing itself (Scripted metric aggs map script)", ex.getMessage());
            }
        }
    }

    public void testSelfReferencingAggStateAfterCombine() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // No need to add docs for this test
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT)
                    .combineScript(COMBINE_SCRIPT_SELF_REF).reduceScript(REDUCE_SCRIPT);

                IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
                    search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder)
                );
                assertEquals("Iterable object is self-referencing itself (Scripted metric aggs combine script)", ex.getMessage());
            }
        }
    }

    /**
     * We cannot use Mockito for mocking QueryShardContext in this case because
     * script-related methods (e.g. QueryShardContext#getLazyExecutableScript)
     * is final and cannot be mocked
     */
    @Override
    protected QueryShardContext queryShardContextMock(MapperService mapperService, IndexSettings indexSettings,
                                                      CircuitBreakerService circuitBreakerService) {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, SCRIPTS, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);
        ScriptService scriptService =  new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
        return new QueryShardContext(0, indexSettings, null, null, null, mapperService, null, scriptService,
                xContentRegistry(), writableRegistry(), null, null, System::currentTimeMillis, null);
    }
}
