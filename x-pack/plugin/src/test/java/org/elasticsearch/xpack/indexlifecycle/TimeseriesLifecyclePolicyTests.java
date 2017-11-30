/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_PHASES;
import static org.hamcrest.Matchers.equalTo;

public class TimeseriesLifecyclePolicyTests extends AbstractSerializingTestCase<LifecyclePolicy> {
    
    private NamedXContentRegistry registry;
    private String lifecycleName;

    @Before
    public void setup() {
        List<NamedXContentRegistry.Entry> entries = Arrays
            .asList(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
                new NamedXContentRegistry.Entry(LifecyclePolicy.class, new ParseField(TimeseriesLifecyclePolicy.TYPE),
                    TimeseriesLifecyclePolicy::parse));
        registry = new NamedXContentRegistry(entries);
        lifecycleName = randomAlphaOfLength(20); // NOCOMMIT we need to randomise the lifecycle name rather 
                                                 // than use the same name for all instances
    }

    @Override
    protected LifecyclePolicy createTestInstance() {
        return new TimeseriesLifecyclePolicy(lifecycleName, Collections.emptyMap());
    }

    @Override
    protected LifecyclePolicy doParseInstance(XContentParser parser) throws IOException {
        return LifecyclePolicy.parse(parser, new Tuple<>(lifecycleName, registry));
    }

    @Override
    protected Reader<LifecyclePolicy> instanceReader() {
        return TimeseriesLifecyclePolicy::new;
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new)));
    }

    public void testGetFirstPhase() {
        Map<String, Phase> phases = new HashMap<>();
        Phase expectedFirstPhase = null;
        for (String phaseName : Arrays.asList("hot", "warm", "cold", "delete")) {
            if (randomBoolean()) {
                Phase phase = new Phase(phaseName, TimeValue.MINUS_ONE, Collections.emptyList());
                phases.put(phaseName, phase);
                if (expectedFirstPhase == null) {
                    expectedFirstPhase = phase;
                }
            }
        }
        TimeseriesLifecyclePolicy policy = new TimeseriesLifecyclePolicy(lifecycleName, phases);
        assertThat(policy.getFirstPhase(), equalTo(expectedFirstPhase));
    }

    public void testGetNextPhase() {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            Map<String, Phase> phases = new HashMap<>();
            List<Phase> phasesInOrder = new ArrayList<>();
            for (String phase : VALID_PHASES) {
                if (randomBoolean()) {
                    Phase phaseToAdd = new Phase(phase, TimeValue.MINUS_ONE, Collections.emptyList());
                    phases.put(phase, phaseToAdd);
                    phasesInOrder.add(phaseToAdd);
                }
            }
            TimeseriesLifecyclePolicy policy = new TimeseriesLifecyclePolicy(lifecycleName, phases);
            assertThat(policy.nextPhase(null), equalTo(policy.getFirstPhase()));
            for (int i = 0; i < phasesInOrder.size() - 1; i++) {
                assertThat(policy.nextPhase(phasesInOrder.get(i)), equalTo(phasesInOrder.get(i + 1)));
            }
            if (phasesInOrder.isEmpty() == false) {
                assertNull(policy.nextPhase(phasesInOrder.get(phasesInOrder.size() - 1)));
            }
        }
    }

    public void testValidate() {
        boolean invalid = randomBoolean();
        String phaseName = randomFrom("hot", "warm", "cold", "delete");
        if (invalid) {
            phaseName += randomAlphaOfLength(5);
        }
        Map<String, Phase> phases = Collections.singletonMap(phaseName,
            new Phase(phaseName, TimeValue.ZERO, Collections.emptyList()));
        if (invalid) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> new TimeseriesLifecyclePolicy(lifecycleName, phases));
            assertThat(e.getMessage(), equalTo("Timeseries lifecycle does not support phase [" + phaseName + "]"));
        } else {
            new TimeseriesLifecyclePolicy(lifecycleName, phases);
        }
    }
}
