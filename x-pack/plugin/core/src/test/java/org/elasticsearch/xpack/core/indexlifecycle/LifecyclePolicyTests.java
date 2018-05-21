/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

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
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class LifecyclePolicyTests extends AbstractSerializingTestCase<LifecyclePolicy> {

    private String lifecycleName;

    @Override
    protected LifecyclePolicy doParseInstance(XContentParser parser) {
        return LifecyclePolicy.parse(parser, lifecycleName);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, MockAction.NAME, MockAction::new),
                        new NamedWriteableRegistry.Entry(LifecycleType.class, TestLifecycleType.TYPE, (in) -> TestLifecycleType.INSTANCE)));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MockAction.NAME), MockAction::parse));
        entries.add(new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TestLifecycleType.TYPE),
                (p) -> TestLifecycleType.INSTANCE));
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected LifecyclePolicy createTestInstance() {
        lifecycleName = randomAlphaOfLength(5);
        return randomLifecyclePolicy(lifecycleName);
    }

    public static LifecyclePolicy randomLifecyclePolicy(@Nullable String lifecycleName) {
        int numberPhases = randomInt(5);
        Map<String, Phase> phases = new HashMap<>(numberPhases);
        for (int i = 0; i < numberPhases; i++) {
            TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
            Map<String, LifecycleAction> actions = new HashMap<>();
            if (randomBoolean()) {
                MockAction action = new MockAction();
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

    public void testFirstAndLastSteps() {
        Client client = mock(Client.class);
        LongSupplier nowSupplier = () -> 0L;
        lifecycleName = randomAlphaOfLengthBetween(1, 20);
        Map<String, Phase> phases = new LinkedHashMap<>();
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases);
        List<Step> steps = policy.toSteps(client, nowSupplier);
        assertThat(steps.size(), equalTo(2));
        assertThat(steps.get(0), instanceOf(InitializePolicyContextStep.class));
        assertThat(steps.get(0).getKey(), equalTo(new StepKey("pre-phase", "pre-action", "init")));
        assertThat(steps.get(0).getNextStepKey(), equalTo(TerminalPolicyStep.KEY));
        assertSame(steps.get(1), TerminalPolicyStep.INSTANCE);
    }

    public void testToStepsWithOneStep() {
        Client client = mock(Client.class);
        LongSupplier nowSupplier = () -> 0L;
        MockStep mockStep = new MockStep(
            new Step.StepKey("test", "test", "test"), TerminalPolicyStep.KEY);

        lifecycleName = randomAlphaOfLengthBetween(1, 20);
        Map<String, Phase> phases = new LinkedHashMap<>();
        LifecycleAction firstAction = new MockAction(Arrays.asList(mockStep));
        Map<String, LifecycleAction> actions = Collections.singletonMap(MockAction.NAME, firstAction);
        Phase firstPhase = new Phase("test", TimeValue.ZERO, actions);
        phases.put(firstPhase.getName(), firstPhase);
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases);
        StepKey firstStepKey = InitializePolicyContextStep.KEY;
        StepKey secondStepKey = new StepKey("new", PhaseAfterStep.NAME, PhaseAfterStep.NAME);
        List<Step> steps = policy.toSteps(client, nowSupplier);
        assertThat(steps.size(), equalTo(4));
        assertSame(steps.get(0).getKey(), firstStepKey);
        assertThat(steps.get(0).getNextStepKey(), equalTo(secondStepKey));
        assertThat(steps.get(1).getKey(), equalTo(secondStepKey));
        assertThat(steps.get(1).getNextStepKey(), equalTo(mockStep.getKey()));
        assertThat(steps.get(2).getKey(), equalTo(mockStep.getKey()));
        assertThat(steps.get(2).getNextStepKey(), equalTo(TerminalPolicyStep.KEY));
        assertSame(steps.get(3), TerminalPolicyStep.INSTANCE);
    }

    public void testToStepsWithTwoPhases() {
        Client client = mock(Client.class);
        LongSupplier nowSupplier = () -> 0L;
        MockStep secondActionStep = new MockStep(new StepKey("second_phase", "test2", "test"), TerminalPolicyStep.KEY);
        MockStep secondAfter = new MockStep(new StepKey("first_phase", PhaseAfterStep.NAME, PhaseAfterStep.NAME),
                secondActionStep.getKey());
        MockStep firstActionAnotherStep = new MockStep(new StepKey("first_phase", "test", "bar"), secondAfter.getKey());
        MockStep firstActionStep = new MockStep(new StepKey("first_phase", "test", "foo"), firstActionAnotherStep.getKey());
        MockStep firstAfter = new MockStep(new StepKey("new", PhaseAfterStep.NAME, PhaseAfterStep.NAME), firstActionStep.getKey());
        MockStep init = new MockStep(InitializePolicyContextStep.KEY, firstAfter.getKey());

        lifecycleName = randomAlphaOfLengthBetween(1, 20);
        Map<String, Phase> phases = new LinkedHashMap<>();
        LifecycleAction firstAction = new MockAction(Arrays.asList(firstActionStep, firstActionAnotherStep));
        LifecycleAction secondAction = new MockAction(Arrays.asList(secondActionStep));
        Map<String, LifecycleAction> firstActions = Collections.singletonMap(MockAction.NAME, firstAction);
        Map<String, LifecycleAction> secondActions = Collections.singletonMap(MockAction.NAME, secondAction);
        Phase firstPhase = new Phase("first_phase", TimeValue.ZERO, firstActions);
        Phase secondPhase = new Phase("second_phase", TimeValue.ZERO, secondActions);
        phases.put(firstPhase.getName(), firstPhase);
        phases.put(secondPhase.getName(), secondPhase);
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases);

        List<Step> steps = policy.toSteps(client, nowSupplier);
        assertThat(steps.size(), equalTo(7));
        assertThat(steps.get(0).getClass(), equalTo(InitializePolicyContextStep.class));
        assertThat(steps.get(0).getKey(), equalTo(init.getKey()));
        assertThat(steps.get(0).getNextStepKey(), equalTo(init.getNextStepKey()));
        assertThat(steps.get(1).getClass(), equalTo(PhaseAfterStep.class));
        assertThat(steps.get(1).getKey(), equalTo(firstAfter.getKey()));
        assertThat(steps.get(1).getNextStepKey(), equalTo(firstAfter.getNextStepKey()));
        assertThat(steps.get(2), equalTo(firstActionStep));
        assertThat(steps.get(3), equalTo(firstActionAnotherStep));
        assertThat(steps.get(4).getClass(), equalTo(PhaseAfterStep.class));
        assertThat(steps.get(4).getKey(), equalTo(secondAfter.getKey()));
        assertThat(steps.get(4).getNextStepKey(), equalTo(secondAfter.getNextStepKey()));
        assertThat(steps.get(5), equalTo(secondActionStep));
        assertSame(steps.get(6), TerminalPolicyStep.INSTANCE);
    }
}
