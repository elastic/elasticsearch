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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.ScoreAccessor;
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

    private static final Script INIT_SCRIPT_SCORE = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "initScriptScore",
            Collections.emptyMap());
    private static final Script MAP_SCRIPT_SCORE = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "mapScriptScore",
            Collections.emptyMap());
    private static final Script COMBINE_SCRIPT_SCORE = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "combineScriptScore",
            Collections.emptyMap());

    private static final Script INIT_SCRIPT_PARAMS = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "initScriptParams",
            Collections.singletonMap("initialValue", 24));
    private static final Script MAP_SCRIPT_PARAMS = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "mapScriptParams",
            Collections.singletonMap("itemValue", 12));
    private static final Script COMBINE_SCRIPT_PARAMS = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "combineScriptParams",
            Collections.singletonMap("divisor", 4));
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

        SCRIPTS.put("initScriptScore", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            agg.put("collector", new ArrayList<Double>());
            return agg;
            });
        SCRIPTS.put("mapScriptScore", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            ((List<Double>) agg.get("collector")).add(((ScoreAccessor) params.get("_score")).doubleValue());
            return agg;
        });
        SCRIPTS.put("combineScriptScore", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            return ((List<Double>) agg.get("collector")).stream().mapToDouble(Double::doubleValue).sum();
        });

        SCRIPTS.put("initScriptParams", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            Integer initialValue = (Integer)params.get("initialValue");
            ArrayList<Integer> collector = new ArrayList<>();
            collector.add(initialValue);
            agg.put("collector", collector);
            return agg;
        });
        SCRIPTS.put("mapScriptParams", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            Integer itemValue = (Integer) params.get("itemValue");
            ((List<Integer>) agg.get("collector")).add(itemValue);
            return agg;
        });
        SCRIPTS.put("combineScriptParams", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            int divisor = ((Integer) params.get("divisor"));
            return ((List<Integer>) agg.get("collector")).stream().mapToInt(Integer::intValue).map(i -> i / divisor).sum();
        });

        SCRIPTS.put("initScriptSelfRef", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            agg.put("collector", new ArrayList<Integer>());
            agg.put("selfRef", agg);
            return agg;
        });

        SCRIPTS.put("mapScriptSelfRef", params -> {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            agg.put("selfRef", agg);
            return agg;
        });

        SCRIPTS.put("combineScriptSelfRef", params -> {
           Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
           agg.put("selfRef", agg);
           return agg;
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
                @SuppressWarnings("unchecked")
                Map<String, Object> agg = (Map<String, Object>) scriptedMetric.aggregation();
                @SuppressWarnings("unchecked")
                List<Integer> list = (List<Integer>) agg.get("collector");
                assertEquals(numDocs, list.size());
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
                aggregationBuilder.initScript(INIT_SCRIPT_SCORE).mapScript(MAP_SCRIPT_SCORE).combineScript(COMBINE_SCRIPT_SCORE);
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
                aggregationBuilder.initScript(INIT_SCRIPT_PARAMS).mapScript(MAP_SCRIPT_PARAMS).combineScript(COMBINE_SCRIPT_PARAMS);
                ScriptedMetric scriptedMetric = search(newSearcher(indexReader, true, true), new MatchAllDocsQuery(), aggregationBuilder);

                // The result value depends on the script params.
                assertEquals(306, scriptedMetric.aggregation());
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
                aggregationBuilder.params(aggParams).initScript(INIT_SCRIPT_PARAMS).mapScript(MAP_SCRIPT_PARAMS).
                    combineScript(COMBINE_SCRIPT_PARAMS);

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
                aggregationBuilder.initScript(INIT_SCRIPT_SELF_REF).mapScript(MAP_SCRIPT);

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
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT_SELF_REF);

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
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT).combineScript(COMBINE_SCRIPT_SELF_REF);

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
    protected QueryShardContext queryShardContextMock(MapperService mapperService, final MappedFieldType[] fieldTypes,
            CircuitBreakerService circuitBreakerService) {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, SCRIPTS);
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);
        ScriptService scriptService =  new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
        return new QueryShardContext(0, mapperService.getIndexSettings(), null, null, mapperService, null, scriptService,
                xContentRegistry(), writableRegistry(), null, null, System::currentTimeMillis, null);
    }
}
