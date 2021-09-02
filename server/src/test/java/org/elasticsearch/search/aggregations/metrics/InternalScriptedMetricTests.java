/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class InternalScriptedMetricTests extends InternalAggregationTestCase<InternalScriptedMetric> {

    private static final String REDUCE_SCRIPT_NAME = "reduceScript";
    private boolean hasReduceScript;
    private Supplier<Object>[] valueTypes;
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private final Supplier<Object>[] leafValueSuppliers = new Supplier[] {
        () -> randomInt(),
        () -> randomLong(),
        () -> randomDouble(),
        () -> randomFloat(),
        () -> randomBoolean(),
        () -> randomAlphaOfLength(5),
        () -> new GeoPoint(randomDouble(), randomDouble()),
        () -> null };
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private final Supplier<Object>[] nestedValueSuppliers = new Supplier[] { () -> new HashMap<String, Object>(), () -> new ArrayList<>() };

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void setUp() throws Exception {
        super.setUp();
        hasReduceScript = randomBoolean();
        // we want the same value types (also for nested lists, maps) for all random aggregations
        int levels = randomIntBetween(1, 3);
        valueTypes = new Supplier[levels];
        for (int i = 0; i < levels; i++) {
            if (i < levels - 1) {
                valueTypes[i] = randomFrom(nestedValueSuppliers);
            } else {
                // the last one needs to be a leaf value, not map or list
                valueTypes[i] = randomFrom(leafValueSuppliers);
            }
        }
    }

    @Override
    protected InternalScriptedMetric createTestInstance(String name, Map<String, Object> metadata) {
        Map<String, Object> params = new HashMap<>();
        if (randomBoolean()) {
            params.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        Script reduceScript = null;
        if (hasReduceScript) {
            reduceScript = new Script(ScriptType.INLINE, MockScriptEngine.NAME, REDUCE_SCRIPT_NAME, params);
        }
        return new InternalScriptedMetric(name, randomAggregations(), reduceScript, metadata);
    }

    private List<Object> randomAggregations() {
        return randomList(randomBoolean() ? 1 : 5, this::randomAggregation);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Object randomAggregation() {
        int levels = randomIntBetween(1, 3);
        Supplier[] valueTypes = new Supplier[levels];
        for (int l = 0; l < levels; l++) {
            if (l < levels - 1) {
                valueTypes[l] = randomFrom(nestedValueSuppliers);
            } else {
                // the last one needs to be a leaf value, not map or
                // list
                valueTypes[l] = randomFrom(leafValueSuppliers);
            }
        }
        return randomValue(valueTypes, 0);
    }

    @SuppressWarnings("unchecked")
    private static Object randomValue(Supplier<Object>[] valueTypes, int level) {
        Object value = valueTypes[level].get();
        if (value instanceof Map) {
            int elements = randomIntBetween(1, 5);
            Map<String, Object> map = (Map<String, Object>) value;
            for (int i = 0; i < elements; i++) {
                map.put(randomAlphaOfLength(5), randomValue(valueTypes, level + 1));
            }
        } else if (value instanceof List) {
            int elements = randomIntBetween(1, 5);
            List<Object> list = (List<Object>) value;
            for (int i = 0; i < elements; i++) {
                list.add(randomValue(valueTypes, level + 1));
            }
        }
        return value;
    }

    /**
     * Mock of the script service. The script that is run looks at the
     * "states" context variable visible when executing the script and simply returns the count.
     * This should be equal to the number of input InternalScriptedMetrics that are reduced
     * in total.
     */
    @Override
    protected ScriptService mockScriptService() {
        // mock script always returns the size of the input aggs list as result
        @SuppressWarnings("unchecked")
        MockScriptEngine scriptEngine = new MockScriptEngine(
            MockScriptEngine.NAME,
            Collections.singletonMap(REDUCE_SCRIPT_NAME, script -> ((List<Object>) script.get("states")).size()),
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    @Override
    protected void assertReduced(InternalScriptedMetric reduced, List<InternalScriptedMetric> inputs) {
        InternalScriptedMetric firstAgg = inputs.get(0);
        assertEquals(firstAgg.getName(), reduced.getName());
        assertEquals(firstAgg.getMetadata(), reduced.getMetadata());
        int size = (int) inputs.stream().mapToLong(i -> i.aggregationsList().size()).sum();
        if (hasReduceScript) {
            assertEquals(size, reduced.aggregation());
        } else {
            assertEquals(size, ((List<?>) reduced.aggregation()).size());
        }
    }

    @Override
    public InternalScriptedMetric createTestInstanceForXContent() {
        InternalScriptedMetric aggregation = createTestInstance();
        return (InternalScriptedMetric) aggregation.reduce(
            singletonList(aggregation),
            ReduceContext.forFinalReduction(null, mockScriptService(), null, PipelineTree.EMPTY)
        );
    }

    @Override
    protected void assertFromXContent(InternalScriptedMetric aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedScriptedMetric);
        ParsedScriptedMetric parsed = (ParsedScriptedMetric) parsedAggregation;

        assertValues(aggregation.aggregation(), parsed.aggregation());
    }

    private static void assertValues(Object expected, Object actual) {
        if (expected instanceof Long) {
            // longs that fit into the integer range are parsed back as integer
            if (actual instanceof Integer) {
                assertEquals(((Long) expected).intValue(), actual);
            } else {
                assertEquals(expected, actual);
            }
        } else if (expected instanceof Float) {
            // based on the xContent type, floats are sometimes parsed back as doubles
            if (actual instanceof Double) {
                assertEquals(expected, ((Double) actual).floatValue());
            } else {
                assertEquals(expected, actual);
            }
        } else if (expected instanceof GeoPoint) {
            assertTrue(actual instanceof Map);
            GeoPoint point = (GeoPoint) expected;
            @SuppressWarnings("unchecked")
            Map<String, Object> pointMap = (Map<String, Object>) actual;
            assertEquals(point.getLat(), pointMap.get("lat"));
            assertEquals(point.getLon(), pointMap.get("lon"));
        } else if (expected instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> expectedMap = (Map<String, Object>) expected;
            @SuppressWarnings("unchecked")
            Map<String, Object> actualMap = (Map<String, Object>) actual;
            assertEquals(expectedMap.size(), actualMap.size());
            for (String key : expectedMap.keySet()) {
                assertValues(expectedMap.get(key), actualMap.get(key));
            }
        } else if (expected instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> expectedList = (List<Object>) expected;
            @SuppressWarnings("unchecked")
            List<Object> actualList = (List<Object>) actual;
            assertEquals(expectedList.size(), actualList.size());
            Iterator<Object> actualIterator = actualList.iterator();
            for (Object element : expectedList) {
                assertValues(element, actualIterator.next());
            }
        } else {
            assertEquals(expected, actual);
        }
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> path.contains(CommonFields.VALUE.getPreferredName());
    }

    @Override
    protected InternalScriptedMetric mutateInstance(InternalScriptedMetric instance) throws IOException {
        String name = instance.getName();
        List<Object> aggregationsList = instance.aggregationsList();
        Script reduceScript = instance.reduceScript;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                aggregationsList = randomValueOtherThan(aggregationsList, this::randomAggregations);
                break;
            case 2:
                reduceScript = new Script(
                    ScriptType.INLINE,
                    MockScriptEngine.NAME,
                    REDUCE_SCRIPT_NAME + "-mutated",
                    Collections.emptyMap()
                );
                break;
            case 3:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalScriptedMetric(name, aggregationsList, reduceScript, metadata);
    }

    public void testOldSerialization() throws IOException {
        // A single element list looks like a fully reduced agg
        InternalScriptedMetric original = new InternalScriptedMetric("test", List.of("foo"), new Script("test"), null);
        InternalScriptedMetric roundTripped = (InternalScriptedMetric) copyNamedWriteable(
            original,
            getNamedWriteableRegistry(),
            InternalAggregation.class,
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, VersionUtils.getPreviousVersion(Version.V_7_8_0))
        );
        assertThat(roundTripped, equalTo(original));

        // A multi-element list looks like a non-reduced agg
        InternalScriptedMetric unreduced = new InternalScriptedMetric("test", List.of("foo", "bar"), new Script("test"), null);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> copyNamedWriteable(
                unreduced,
                getNamedWriteableRegistry(),
                InternalAggregation.class,
                VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, VersionUtils.getPreviousVersion(Version.V_7_8_0))
            )
        );
        assertThat(e.getMessage(), equalTo("scripted_metric doesn't support cross cluster search until 7.8.0"));
    }
}
