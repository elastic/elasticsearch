/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptedMetricAggregatorTests extends AggregatorTestCase {

    private static final String AGG_NAME = "scriptedMetric";
    private static final Script INIT_SCRIPT = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "initScript", Collections.emptyMap());
    private static final Script MAP_SCRIPT = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "mapScript", Collections.emptyMap());
    private static final Script COMBINE_SCRIPT = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "combineScript",
        Collections.emptyMap()
    );
    private static final Script REDUCE_SCRIPT = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "reduceScript",
        Collections.emptyMap()
    );

    private static final Script INIT_SCRIPT_SCORE = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "initScriptScore",
        Collections.emptyMap()
    );
    private static final Script MAP_SCRIPT_SCORE = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "mapScriptScore",
        Collections.emptyMap()
    );
    private static final Script COMBINE_SCRIPT_SCORE = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "combineScriptScore",
        Collections.emptyMap()
    );
    private static final Script COMBINE_SCRIPT_NOOP = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "combineScriptNoop",
        Collections.emptyMap()
    );

    private static final Script INIT_SCRIPT_PARAMS = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "initScriptParams",
        Collections.singletonMap("initialValue", 24)
    );
    private static final Script MAP_SCRIPT_PARAMS = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "mapScriptParams",
        Collections.singletonMap("itemValue", 12)
    );
    private static final Script COMBINE_SCRIPT_PARAMS = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "combineScriptParams",
        Collections.singletonMap("multiplier", 4)
    );
    private static final Script REDUCE_SCRIPT_PARAMS = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "reduceScriptParams",
        Collections.singletonMap("additional", 2)
    );
    private static final String CONFLICTING_PARAM_NAME = "initialValue";

    private static final Script INIT_SCRIPT_SELF_REF = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "initScriptSelfRef",
        Collections.emptyMap()
    );
    private static final Script MAP_SCRIPT_SELF_REF = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "mapScriptSelfRef",
        Collections.emptyMap()
    );
    private static final Script COMBINE_SCRIPT_SELF_REF = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "combineScriptSelfRef",
        Collections.emptyMap()
    );

    private static final Script INIT_SCRIPT_MAKING_ARRAY = new Script(
        ScriptType.INLINE,
        MockScriptEngine.NAME,
        "initScriptMakingArray",
        Collections.emptyMap()
    );

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
            List<?> states = (List<?>) params.get("states");
            return states.stream().filter(a -> a instanceof Number).map(a -> (Number) a).mapToInt(Number::intValue).sum();
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
            Integer initialValue = (Integer) params.get("initialValue");
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
        SCRIPTS.put(
            "reduceScriptParams",
            params -> ((List) params.get("states")).stream().mapToInt(i -> (int) i).sum() + (int) params.get("aggs_param") + (int) params
                .get("additional") - ((List) params.get("states")).size() * 24 * 4
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
        SCRIPTS.put("initScriptMakingArray", params -> {
            Map<String, Object> state = (Map<String, Object>) params.get("state");
            state.put("array", new String[] { "foo", "bar" });
            state.put("collector", new ArrayList<Integer>());
            return state;
        });
    }

    private CircuitBreakerService circuitBreakerService;

    @Before
    public void mockBreaker() {
        circuitBreakerService = mock(CircuitBreakerService.class);
        when(circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(new NoopCircuitBreaker(CircuitBreaker.REQUEST) {
            private long total = 0;

            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                logger.debug("Used {} grabbing {} for {}", total, bytes, label);
                total += bytes;
            }

            @Override
            public void addWithoutBreaking(long bytes) {
                logger.debug("Used {} grabbing {}", total, bytes);
                total += bytes;
            }

            @Override
            public long getUsed() {
                return total;
            }
        });
    }

    @Override
    protected void afterClose() {
        assertThat(circuitBreakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    @Override
    protected ScriptService getMockScriptService() {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, SCRIPTS, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    @SuppressWarnings("unchecked")
    public void testNoDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // intentionally not writing any docs
            }
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.mapScript(MAP_SCRIPT).combineScript(COMBINE_SCRIPT_NOOP).reduceScript(REDUCE_SCRIPT);
                ScriptedMetric scriptedMetric = searchAndReduce(newIndexSearcher(indexReader), new AggTestConfig(aggregationBuilder));
                assertEquals(AGG_NAME, scriptedMetric.getName());
                assertNotNull(scriptedMetric.aggregation());
                assertEquals(0, scriptedMetric.aggregation());
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
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT).reduceScript(REDUCE_SCRIPT);
                IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
                    searchAndReduce(newIndexSearcher(indexReader), new AggTestConfig(aggregationBuilder));
                });
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
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT).combineScript(COMBINE_SCRIPT);
                IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
                    searchAndReduce(newIndexSearcher(indexReader), new AggTestConfig(aggregationBuilder));
                });
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
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT).mapScript(MAP_SCRIPT).combineScript(COMBINE_SCRIPT).reduceScript(REDUCE_SCRIPT);
                ScriptedMetric scriptedMetric = searchAndReduce(newIndexSearcher(indexReader), new AggTestConfig(aggregationBuilder));
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
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT_SCORE)
                    .mapScript(MAP_SCRIPT_SCORE)
                    .combineScript(COMBINE_SCRIPT_SCORE)
                    .reduceScript(REDUCE_SCRIPT);
                ScriptedMetric scriptedMetric = searchAndReduce(newIndexSearcher(indexReader), new AggTestConfig(aggregationBuilder));
                assertEquals(AGG_NAME, scriptedMetric.getName());
                assertNotNull(scriptedMetric.aggregation());
                // all documents have score of 1.0
                assertEquals(numDocs, scriptedMetric.aggregation());
            }
        }
    }

    public void testScriptParamsPassedThrough() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 100; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
                // force a single aggregator
                indexWriter.forceMerge(1);
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT_PARAMS)
                    .mapScript(MAP_SCRIPT_PARAMS)
                    .combineScript(COMBINE_SCRIPT_PARAMS)
                    .reduceScript(REDUCE_SCRIPT);
                ScriptedMetric scriptedMetric = searchAndReduce(newIndexSearcher(indexReader), new AggTestConfig(aggregationBuilder));

                // The result value depends on the script params.
                assertEquals(4896, scriptedMetric.aggregation());
            }
        }
    }

    public void testAggParamsPassedToReduceScript() throws IOException {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, SCRIPTS, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);
        ScriptService scriptService = new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 100; i++) {
                    indexWriter.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.params(Collections.singletonMap("aggs_param", 1))
                    .initScript(INIT_SCRIPT_PARAMS)
                    .mapScript(MAP_SCRIPT_PARAMS)
                    .combineScript(COMBINE_SCRIPT_PARAMS)
                    .reduceScript(REDUCE_SCRIPT_PARAMS);
                ScriptedMetric scriptedMetric = searchAndReduce(
                    newIndexSearcher(indexReader),
                    new AggTestConfig(aggregationBuilder).withMaxBuckets(0)
                );

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

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                Map<String, Object> aggParams = Collections.singletonMap(CONFLICTING_PARAM_NAME, "blah");
                aggregationBuilder.params(aggParams)
                    .initScript(INIT_SCRIPT_PARAMS)
                    .mapScript(MAP_SCRIPT_PARAMS)
                    .combineScript(COMBINE_SCRIPT_PARAMS)
                    .reduceScript(REDUCE_SCRIPT);

                IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
                    searchAndReduce(newIndexSearcher(indexReader), new AggTestConfig(aggregationBuilder));
                });
                assertEquals(
                    "Parameter name \"" + CONFLICTING_PARAM_NAME + "\" used in both aggregation and script parameters",
                    ex.getMessage()
                );
            }
        }
    }

    public void testSelfReferencingAggStateAfterInit() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // No need to add docs for this test
            }
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT_SELF_REF)
                    .mapScript(MAP_SCRIPT)
                    .combineScript(COMBINE_SCRIPT_PARAMS)
                    .reduceScript(REDUCE_SCRIPT);

                IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
                    searchAndReduce(newIndexSearcher(indexReader), new AggTestConfig(aggregationBuilder));
                });
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
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT)
                    .mapScript(MAP_SCRIPT_SELF_REF)
                    .combineScript(COMBINE_SCRIPT_PARAMS)
                    .reduceScript(REDUCE_SCRIPT);

                IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
                    searchAndReduce(newIndexSearcher(indexReader), new AggTestConfig(aggregationBuilder));
                });
                assertEquals("Iterable object is self-referencing itself (Scripted metric aggs map script)", ex.getMessage());
            }
        }
    }

    public void testSelfReferencingAggStateAfterCombine() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // No need to add docs for this test
            }
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
                aggregationBuilder.initScript(INIT_SCRIPT)
                    .mapScript(MAP_SCRIPT)
                    .combineScript(COMBINE_SCRIPT_SELF_REF)
                    .reduceScript(REDUCE_SCRIPT);

                IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
                    searchAndReduce(newIndexSearcher(indexReader), new AggTestConfig(aggregationBuilder));
                });
                assertEquals("Iterable object is self-referencing itself (Scripted metric aggs combine script)", ex.getMessage());
            }
        }
    }

    public void testInitScriptMakesArray() throws IOException {
        ScriptedMetricAggregationBuilder aggregationBuilder = new ScriptedMetricAggregationBuilder(AGG_NAME);
        aggregationBuilder.initScript(INIT_SCRIPT_MAKING_ARRAY)
            .mapScript(MAP_SCRIPT)
            .combineScript(COMBINE_SCRIPT)
            .reduceScript(REDUCE_SCRIPT);
        testCase(
            iw -> { iw.addDocument(new Document()); },
            (InternalScriptedMetric r) -> { assertEquals(1, r.aggregation()); },
            new AggTestConfig(aggregationBuilder)
        );
    }

    public void testAsSubAgg() throws IOException {
        AggregationBuilder aggregationBuilder = new TermsAggregationBuilder("t").field("t")
            .executionHint("map")
            .subAggregation(
                new ScriptedMetricAggregationBuilder("scripted").initScript(INIT_SCRIPT)
                    .mapScript(MAP_SCRIPT)
                    .combineScript(COMBINE_SCRIPT)
                    .reduceScript(REDUCE_SCRIPT)
            );
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 99; i++) {
                iw.addDocument(singleton(new SortedSetDocValuesField("t", i % 2 == 0 ? new BytesRef("even") : new BytesRef("odd"))));
            }
        };
        Consumer<StringTerms> verify = terms -> {
            StringTerms.Bucket even = terms.getBucketByKey("even");
            assertThat(even.getDocCount(), equalTo(50L));
            ScriptedMetric evenMetric = even.getAggregations().get("scripted");
            assertThat(evenMetric.aggregation(), equalTo(50));
            StringTerms.Bucket odd = terms.getBucketByKey("odd");
            assertThat(odd.getDocCount(), equalTo(49L));
            ScriptedMetric oddMetric = odd.getAggregations().get("scripted");
            assertThat(oddMetric.aggregation(), equalTo(49));
        };
        testCase(buildIndex, verify, new AggTestConfig(aggregationBuilder, keywordField("t"), longField("number")));
    }
}
