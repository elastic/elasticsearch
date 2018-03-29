/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAsyncActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.MockAction;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.TestLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class LifecyclePolicyTests extends AbstractSerializingTestCase<LifecyclePolicy> {

    private String indexName;
    private String lifecycleName;
    private MockAction firstAction;
    private MockAction secondAction;
    private MockAction thirdAction;
    private Phase firstPhase;
    private Phase secondPhase;
    private Phase thirdPhase;
    private LifecyclePolicy policy;

    @Override
    protected LifecyclePolicy doParseInstance(XContentParser parser) throws IOException {
        return LifecyclePolicy.parse(parser, lifecycleName);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new),
                        new NamedWriteableRegistry.Entry(LifecycleType.class, TestLifecycleType.TYPE, (in) -> TestLifecycleType.INSTANCE)));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        entries.add(new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TestLifecycleType.TYPE),
                (p) -> TestLifecycleType.INSTANCE));
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected LifecyclePolicy createTestInstance() {
        int numberPhases = randomInt(5);
        Map<String, Phase> phases = new HashMap<>(numberPhases);
        for (int i = 0; i < numberPhases; i++) {
            TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
            Map<String, LifecycleAction> actions = new HashMap<>();
            if (randomBoolean()) {
                DeleteAction action = new DeleteAction();
                actions.put(action.getWriteableName(), action);
            }
            String phaseName = randomAlphaOfLength(10);
            phases.put(phaseName, new Phase(phaseName, after, actions));
        }
        return new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases);
    }

    @Override
    protected LifecyclePolicy mutateInstance(LifecyclePolicy instance) throws IOException {
        String name = instance.getName();
        Map<String, Phase> phases = instance.getPhases();
        switch (between(0, 1)) {
        case 0:
            name = name + randomAlphaOfLengthBetween(1, 5);
            break;
        case 1:
            phases = new LinkedHashMap<>(phases);
            String phaseName = randomAlphaOfLengthBetween(1, 10);
            phases.put(phaseName, new Phase(phaseName, TimeValue.timeValueSeconds(randomIntBetween(1, 1000)), Collections.emptyMap()));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new LifecyclePolicy(TestLifecycleType.INSTANCE, name, phases);
    }

    @Override
    protected Reader<LifecyclePolicy> instanceReader() {
        return LifecyclePolicy::new;
    }

    public void testDefaultLifecycleType() {
        LifecyclePolicy policy = new LifecyclePolicy(null, randomAlphaOfLength(10), Collections.emptyMap());
        assertSame(TimeseriesLifecycleType.INSTANCE, policy.getType());
    }

    public void testToSteps() throws Exception {
        Client client = mock(Client.class);
        LongSupplier nowSupplier = () -> 0L;
        Step deleteStep = new DeleteAsyncActionStep(
            new Step.StepKey("delete", "DELETE", "delete"), null, client);

        indexName = randomAlphaOfLengthBetween(1, 20);
        lifecycleName = randomAlphaOfLengthBetween(1, 20);
        Map<String, Phase> phases = new LinkedHashMap<>();
        firstAction = new MockAction(Arrays.asList(deleteStep));
        Map<String, LifecycleAction> actions = Collections.singletonMap(MockAction.NAME, firstAction);
        firstPhase = new Phase("delete", TimeValue.ZERO, actions);
        phases.put(firstPhase.getName(), firstPhase);
        policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases);

        List<Step> steps = policy.toSteps(client, nowSupplier);
        assertThat(steps.size(), equalTo(2));
        assertThat(steps.get(0).getKey(), equalTo(new Step.StepKey("delete", null, "after")));
        assertThat(steps.get(0).getNextStepKey(), equalTo(deleteStep.getKey()));
        assertThat(steps.get(1), equalTo(deleteStep));
        assertNull(steps.get(1).getNextStepKey());
    }
}
