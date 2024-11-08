/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;

public class InternalScriptedMetricTests extends InternalAggregationTestCase<InternalScriptedMetric> {

    private static final String REDUCE_SCRIPT_NAME = "reduceScript";
    private boolean hasReduceScript;
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
    private final Supplier<Object>[] nestedValueSuppliers = new Supplier[] { HashMap::new, ArrayList::new };

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void setUp() throws Exception {
        super.setUp();
        hasReduceScript = randomBoolean();
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
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    @Override
    protected void assertReduced(InternalScriptedMetric reduced, List<InternalScriptedMetric> inputs) {
        // we cannot check the current attribute values as they depend on the first aggregator during the reduced phase
        int size = (int) inputs.stream().mapToLong(i -> i.aggregationsList().size()).sum();
        if (hasReduceScript) {
            assertEquals(size, reduced.aggregation());
        } else {
            assertEquals(size, ((List<?>) reduced.aggregation()).size());
        }
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalScriptedMetric sampled, InternalScriptedMetric reduced, SamplingContext samplingContext) {
        // Nothing to check
    }

    @Override
    public InternalScriptedMetric createTestInstanceForXContent() {
        InternalScriptedMetric aggregation = createTestInstance();
        return (InternalScriptedMetric) InternalAggregationTestCase.reduce(
            singletonList(aggregation),
            new AggregationReduceContext.ForFinal(
                null,
                mockScriptService(),
                () -> false,
                mock(AggregationBuilder.class),
                null,
                PipelineTree.EMPTY
            )
        );
    }

    @Override
    protected InternalScriptedMetric mutateInstance(InternalScriptedMetric instance) {
        String name = instance.getName();
        List<Object> aggregationsList = instance.aggregationsList();
        Script reduceScript = instance.reduceScript;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> aggregationsList = randomValueOtherThan(aggregationsList, this::randomAggregations);
            case 2 -> reduceScript = new Script(
                ScriptType.INLINE,
                MockScriptEngine.NAME,
                REDUCE_SCRIPT_NAME + "-mutated",
                Collections.emptyMap()
            );
            case 3 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalScriptedMetric(name, aggregationsList, reduceScript, metadata);
    }

}
