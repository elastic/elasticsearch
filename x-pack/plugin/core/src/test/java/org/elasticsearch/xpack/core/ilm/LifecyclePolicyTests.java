/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

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
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
            Arrays.asList(
                new NamedWriteableRegistry.Entry(LifecycleType.class, TimeseriesLifecycleType.TYPE,
                    (in) -> TimeseriesLifecycleType.INSTANCE),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, AllocateAction.NAME, AllocateAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, WaitForSnapshotAction.NAME, WaitForSnapshotAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ReadOnlyAction.NAME, ReadOnlyAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, FreezeAction.NAME, FreezeAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, SetPriorityAction.NAME, SetPriorityAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, UnfollowAction.NAME, UnfollowAction::new)
            ));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(Arrays.asList(
            new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TimeseriesLifecycleType.TYPE),
                (p) -> TimeseriesLifecycleType.INSTANCE),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(WaitForSnapshotAction.NAME), WaitForSnapshotAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse)
        ));
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected LifecyclePolicy createTestInstance() {
        lifecycleName = randomAlphaOfLength(5);
        return randomTimeseriesLifecyclePolicy(lifecycleName);
    }

    /**
     * The same as {@link #randomTimeseriesLifecyclePolicy(String)} but ensures
     * that the resulting policy has all valid phases and all valid actions.
     */
    public static LifecyclePolicy randomTimeseriesLifecyclePolicyWithAllPhases(@Nullable String lifecycleName) {
        List<String> phaseNames = TimeseriesLifecycleType.VALID_PHASES;
        Map<String, Phase> phases = new HashMap<>(phaseNames.size());
        Function<String, Set<String>> validActions = (phase) ->  {
            switch (phase) {
                case "hot":
                    return TimeseriesLifecycleType.VALID_HOT_ACTIONS;
                case "warm":
                    return TimeseriesLifecycleType.VALID_WARM_ACTIONS;
                case "cold":
                    return TimeseriesLifecycleType.VALID_COLD_ACTIONS;
                case "delete":
                    return TimeseriesLifecycleType.VALID_DELETE_ACTIONS;
                default:
                    throw new IllegalArgumentException("invalid phase [" + phase + "]");
            }};
        Function<String, LifecycleAction> randomAction = (action) ->  {
            switch (action) {
                case AllocateAction.NAME:
                    return AllocateActionTests.randomInstance();
                case DeleteAction.NAME:
                    return new DeleteAction();
                case WaitForSnapshotAction.NAME:
                    return  WaitForSnapshotActionTests.randomInstance();
                case ForceMergeAction.NAME:
                    return ForceMergeActionTests.randomInstance();
                case ReadOnlyAction.NAME:
                    return new ReadOnlyAction();
                case RolloverAction.NAME:
                    return RolloverActionTests.randomInstance();
                case ShrinkAction.NAME:
                    return ShrinkActionTests.randomInstance();
                case FreezeAction.NAME:
                    return new FreezeAction();
                case SetPriorityAction.NAME:
                    return SetPriorityActionTests.randomInstance();
                case UnfollowAction.NAME:
                    return new UnfollowAction();
                default:
                    throw new IllegalArgumentException("invalid action [" + action + "]");
            }};
        for (String phase : phaseNames) {
            TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
            Map<String, LifecycleAction> actions = new HashMap<>();
            Set<String> actionNames = validActions.apply(phase);
            for (String action : actionNames) {
                actions.put(action, randomAction.apply(action));
            }
            phases.put(phase, new Phase(phase, after, actions));
        }
        return new LifecyclePolicy(TimeseriesLifecycleType.INSTANCE, lifecycleName, phases);
    }

    public static LifecyclePolicy randomTimeseriesLifecyclePolicy(@Nullable String lifecycleName) {
        List<String> phaseNames = randomSubsetOf(
            between(0, TimeseriesLifecycleType.VALID_PHASES.size() - 1), TimeseriesLifecycleType.VALID_PHASES);
        Map<String, Phase> phases = new HashMap<>(phaseNames.size());
        Function<String, Set<String>> validActions = (phase) ->  {
            switch (phase) {
                case "hot":
                    return TimeseriesLifecycleType.VALID_HOT_ACTIONS;
                case "warm":
                    return TimeseriesLifecycleType.VALID_WARM_ACTIONS;
                case "cold":
                    return TimeseriesLifecycleType.VALID_COLD_ACTIONS;
                case "delete":
                    return TimeseriesLifecycleType.VALID_DELETE_ACTIONS;
                default:
                    throw new IllegalArgumentException("invalid phase [" + phase + "]");
            }};
        Function<String, LifecycleAction> randomAction = (action) ->  {
            switch (action) {
                case AllocateAction.NAME:
                    return AllocateActionTests.randomInstance();
                case WaitForSnapshotAction.NAME:
                    return WaitForSnapshotActionTests.randomInstance();
                case DeleteAction.NAME:
                    return new DeleteAction();
                case ForceMergeAction.NAME:
                    return ForceMergeActionTests.randomInstance();
                case ReadOnlyAction.NAME:
                    return new ReadOnlyAction();
                case RolloverAction.NAME:
                    return RolloverActionTests.randomInstance();
                case ShrinkAction.NAME:
                    return ShrinkActionTests.randomInstance();
                case FreezeAction.NAME:
                    return new FreezeAction();
                case SetPriorityAction.NAME:
                    return SetPriorityActionTests.randomInstance();
                case UnfollowAction.NAME:
                    return new UnfollowAction();
                default:
                    throw new IllegalArgumentException("invalid action [" + action + "]");
            }};
        for (String phase : phaseNames) {
            TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
            Map<String, LifecycleAction> actions = new HashMap<>();
            List<String> actionNames = randomSubsetOf(validActions.apply(phase));
            for (String action : actionNames) {
                actions.put(action, randomAction.apply(action));
            }
            phases.put(phase, new Phase(phase, after, actions));
        }
        return new LifecyclePolicy(TimeseriesLifecycleType.INSTANCE, lifecycleName, phases);
    }

    public static LifecyclePolicy randomTestLifecyclePolicy(@Nullable String lifecycleName) {
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
            String phaseName = randomValueOtherThanMany(phases::containsKey, () -> randomFrom(TimeseriesLifecycleType.VALID_PHASES));
            phases = new LinkedHashMap<>(phases);
            phases.put(phaseName, new Phase(phaseName, TimeValue.timeValueSeconds(randomIntBetween(1, 1000)), Collections.emptyMap()));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new LifecyclePolicy(TimeseriesLifecycleType.INSTANCE, name, phases);
    }

    @Override
    protected Reader<LifecyclePolicy> instanceReader() {
        return LifecyclePolicy::new;
    }

    public void testFirstAndLastSteps() {
        Client client = mock(Client.class);
        lifecycleName = randomAlphaOfLengthBetween(1, 20);
        Map<String, Phase> phases = new LinkedHashMap<>();
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases);
        List<Step> steps = policy.toSteps(client);
        assertThat(steps.size(), equalTo(2));
        assertThat(steps.get(0), instanceOf(InitializePolicyContextStep.class));
        assertThat(steps.get(0).getKey(), equalTo(new StepKey("new", "init", "init")));
        assertThat(steps.get(0).getNextStepKey(), equalTo(PhaseCompleteStep.finalStep("new").getKey()));
        assertThat(steps.get(1), equalTo(PhaseCompleteStep.finalStep("new")));
    }

    public void testToStepsWithOneStep() {
        Client client = mock(Client.class);
        MockStep mockStep = new MockStep(
            new Step.StepKey("test", "test", "test"), PhaseCompleteStep.finalStep("test").getKey());

        lifecycleName = randomAlphaOfLengthBetween(1, 20);
        Map<String, Phase> phases = new LinkedHashMap<>();
        LifecycleAction firstAction = new MockAction(Arrays.asList(mockStep));
        Map<String, LifecycleAction> actions = Collections.singletonMap(MockAction.NAME, firstAction);
        Phase firstPhase = new Phase("test", TimeValue.ZERO, actions);
        phases.put(firstPhase.getName(), firstPhase);
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases);
        StepKey firstStepKey = InitializePolicyContextStep.KEY;
        StepKey secondStepKey = PhaseCompleteStep.finalStep("new").getKey();
        List<Step> steps = policy.toSteps(client);
        assertThat(steps.size(), equalTo(4));
        assertSame(steps.get(0).getKey(), firstStepKey);
        assertThat(steps.get(0).getNextStepKey(), equalTo(secondStepKey));
        assertThat(steps.get(1).getKey(), equalTo(secondStepKey));
        assertThat(steps.get(1).getNextStepKey(), equalTo(mockStep.getKey()));
        assertThat(steps.get(2).getKey(), equalTo(mockStep.getKey()));
        assertThat(steps.get(2).getNextStepKey(), equalTo(PhaseCompleteStep.finalStep("test").getKey()));
        assertThat(steps.get(3), equalTo(PhaseCompleteStep.finalStep("test")));
    }

    public void testToStepsWithTwoPhases() {
        Client client = mock(Client.class);
        MockStep secondActionStep = new MockStep(new StepKey("second_phase", "test2", "test"),
            PhaseCompleteStep.finalStep("second_phase").getKey());
        MockStep secondAfter = new MockStep(new StepKey("first_phase", PhaseCompleteStep.NAME, PhaseCompleteStep.NAME),
                secondActionStep.getKey());
        MockStep firstActionAnotherStep = new MockStep(new StepKey("first_phase", "test", "bar"), secondAfter.getKey());
        MockStep firstActionStep = new MockStep(new StepKey("first_phase", "test", "foo"), firstActionAnotherStep.getKey());
        MockStep firstAfter = new MockStep(new StepKey("new", PhaseCompleteStep.NAME, PhaseCompleteStep.NAME), firstActionStep.getKey());
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

        List<Step> steps = policy.toSteps(client);
        assertThat(steps.size(), equalTo(7));
        assertThat(steps.get(0).getClass(), equalTo(InitializePolicyContextStep.class));
        assertThat(steps.get(0).getKey(), equalTo(init.getKey()));
        assertThat(steps.get(0).getNextStepKey(), equalTo(init.getNextStepKey()));
        assertThat(steps.get(1).getClass(), equalTo(PhaseCompleteStep.class));
        assertThat(steps.get(1).getKey(), equalTo(firstAfter.getKey()));
        assertThat(steps.get(1).getNextStepKey(), equalTo(firstAfter.getNextStepKey()));
        assertThat(steps.get(2), equalTo(firstActionStep));
        assertThat(steps.get(3), equalTo(firstActionAnotherStep));
        assertThat(steps.get(4).getClass(), equalTo(PhaseCompleteStep.class));
        assertThat(steps.get(4).getKey(), equalTo(secondAfter.getKey()));
        assertThat(steps.get(4).getNextStepKey(), equalTo(secondAfter.getNextStepKey()));
        assertThat(steps.get(5), equalTo(secondActionStep));
        assertThat(steps.get(6), equalTo(PhaseCompleteStep.finalStep("second_phase")));
    }

    public void testIsActionSafe() {
        Map<String, Phase> phases = new LinkedHashMap<>();
        LifecycleAction firstAction = new MockAction(Collections.emptyList(), true);
        LifecycleAction secondAction = new MockAction(Collections.emptyList(), false);
        Map<String, LifecycleAction> firstActions = Collections.singletonMap(MockAction.NAME, firstAction);
        Map<String, LifecycleAction> secondActions = Collections.singletonMap(MockAction.NAME, secondAction);
        Phase firstPhase = new Phase("first_phase", TimeValue.ZERO, firstActions);
        Phase secondPhase = new Phase("second_phase", TimeValue.ZERO, secondActions);
        phases.put(firstPhase.getName(), firstPhase);
        phases.put(secondPhase.getName(), secondPhase);
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases);

        assertTrue(policy.isActionSafe(new StepKey("first_phase", MockAction.NAME, randomAlphaOfLength(10))));

        assertFalse(policy.isActionSafe(new StepKey("second_phase", MockAction.NAME, randomAlphaOfLength(10))));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> policy.isActionSafe(new StepKey("non_existant_phase", MockAction.NAME, randomAlphaOfLength(10))));
        assertEquals("Phase [non_existant_phase]  does not exist in policy [" + policy.getName() + "]", exception.getMessage());

        exception = expectThrows(IllegalArgumentException.class,
                () -> policy.isActionSafe(new StepKey("first_phase", "non_existant_action", randomAlphaOfLength(10))));
        assertEquals("Action [non_existant_action] in phase [first_phase]  does not exist in policy [" + policy.getName() + "]",
                exception.getMessage());

        assertTrue(policy.isActionSafe(new StepKey("new", randomAlphaOfLength(10), randomAlphaOfLength(10))));
    }

    public void testValidatePolicyName() {
        expectThrows(IllegalArgumentException.class, () -> LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(0,10) +
            "," + randomAlphaOfLengthBetween(0,10)));
        expectThrows(IllegalArgumentException.class, () -> LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(0,10) +
            " " + randomAlphaOfLengthBetween(0,10)));
        expectThrows(IllegalArgumentException.class, () -> LifecyclePolicy.validatePolicyName("_" + randomAlphaOfLengthBetween(1, 20)));
        expectThrows(IllegalArgumentException.class, () -> LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(256, 1000)));

        LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(1,10) + "_" + randomAlphaOfLengthBetween(0,10));

        LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(0,10) + "-" + randomAlphaOfLengthBetween(0,10));
        LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(0,10) + "+" + randomAlphaOfLengthBetween(0,10));

        LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(1,255));
    }
}
