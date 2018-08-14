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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

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
                new NamedWriteableRegistry.Entry(LifecycleAction.class, MockAction.NAME, MockAction::new),
                new NamedWriteableRegistry.Entry(LifecycleType.class, TimeseriesLifecycleType.TYPE,
                    (in) -> TimeseriesLifecycleType.INSTANCE),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, AllocateAction.NAME, AllocateAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ReadOnlyAction.NAME, ReadOnlyAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new)
            ));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(Arrays.asList(
            new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TimeseriesLifecycleType.TYPE),
                (p) -> TimeseriesLifecycleType.INSTANCE),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse)
        ));
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected LifecyclePolicy createTestInstance() {
        lifecycleName = randomAlphaOfLength(5);
        return randomTimeseriesLifecyclePolicy(lifecycleName);
    }

    public static LifecyclePolicy randomTimeseriesLifecyclePolicy(@Nullable String lifecycleName) {
        List<String> phaseNames = randomSubsetOf(TimeseriesLifecycleType.VALID_PHASES);
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
                case ForceMergeAction.NAME:
                    return ForceMergeActionTests.randomInstance();
                case ReadOnlyAction.NAME:
                    return new ReadOnlyAction();
                case RolloverAction.NAME:
                    return RolloverActionTests.randomInstance();
                case ShrinkAction.NAME:
                    return ShrinkActionTests.randomInstance();
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

    public void testGetNextValidStep() {
        List<String> orderedPhases = Arrays.asList("phase_1", "phase_2", "phase_3", "phase_4", "phase_5");
        Map<String, List<String>> orderedActionNamesForPhases = new HashMap<>();
        List<String> actionNamesforPhase = new ArrayList<>();
        actionNamesforPhase.add("action_1");
        actionNamesforPhase.add("action_2");
        actionNamesforPhase.add("action_3");
        actionNamesforPhase.add("action_4");
        orderedActionNamesForPhases.put("phase_1", actionNamesforPhase);
        orderedActionNamesForPhases.put("phase_2", actionNamesforPhase);
        orderedActionNamesForPhases.put("phase_3", actionNamesforPhase);
        orderedActionNamesForPhases.put("phase_4", actionNamesforPhase);
        orderedActionNamesForPhases.put("phase_5", actionNamesforPhase);
        LifecycleType lifecycleType = new ControllableLifecycleType(orderedPhases, orderedActionNamesForPhases);

        // create a policy which has only phases 1,2, and 4 and within them has
        // actions 1 and 3 which both contain steps 1, 2, and 3.
        Map<String, Phase> phases = new HashMap<>();
        for (int p = 1; p <= 4; p++) {
            if (p == 3) {
                continue;
            }
            String phaseName = "phase_" + p;
            Map<String, LifecycleAction> actions = new HashMap<>();

            for (int a = 1; a <= 3; a++) {
                if (a == 2) {
                    continue;
                }
                String actionName = "action_" + a;
                List<Step> steps = new ArrayList<>();
                for (int s = 1; s <= 3; s++) {
                    String stepName = "step_" + s;
                    steps.add(new MockStep(new StepKey(phaseName, actionName, stepName), null));
                }
                NamedMockAction action = new NamedMockAction(actionName, steps);
                actions.put(action.getWriteableName(), action);
            }

            Phase phase = new Phase(phaseName, TimeValue.ZERO, actions);
            phases.put(phase.getName(), phase);
        }
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleType, lifecycleName, phases);

        // step still exists
        StepKey currentStep = new StepKey(randomFrom("phase_1", "phase_2", "phase_4"), randomFrom("action_1", "action_3"),
                randomFrom("step_1", "step_2", "step_3"));
        StepKey nextStep = policy.getNextValidStep(currentStep);
        assertNotNull(nextStep);
        assertEquals(currentStep, nextStep);

        // current action exists but step does not
        currentStep = new StepKey("phase_1", "action_1", "step_missing");
        nextStep = policy.getNextValidStep(currentStep);
        assertNotNull(nextStep);
        assertEquals(new StepKey("phase_1", "action_3", "step_1"), nextStep);

        // current action exists but step does not and action is last in phase
        currentStep = new StepKey("phase_1", "action_3", "step_missing");
        nextStep = policy.getNextValidStep(currentStep);
        assertNotNull(nextStep);
        assertEquals(new StepKey("phase_1", PhaseAfterStep.NAME, PhaseAfterStep.NAME), nextStep);

        // current action exists but step does not and action is last in the
        // last phase
        currentStep = new StepKey("phase_4", "action_3", "step_missing");
        nextStep = policy.getNextValidStep(currentStep);
        assertNotNull(nextStep);
        assertEquals(TerminalPolicyStep.KEY, nextStep);

        // current action no longer exists
        currentStep = new StepKey("phase_1", "action_2", "step_2");
        nextStep = policy.getNextValidStep(currentStep);
        assertNotNull(nextStep);
        assertEquals(new StepKey("phase_1", "action_3", "step_1"), nextStep);

        // current action no longer exists and action was last in phase
        currentStep = new StepKey("phase_1", "action_4", "step_2");
        nextStep = policy.getNextValidStep(currentStep);
        assertNotNull(nextStep);
        assertEquals(new StepKey("phase_1", PhaseAfterStep.NAME, PhaseAfterStep.NAME), nextStep);

        // current action no longer exists and action was last in the last phase
        currentStep = new StepKey("phase_4", "action_4", "step_2");
        nextStep = policy.getNextValidStep(currentStep);
        assertNotNull(nextStep);
        assertEquals(TerminalPolicyStep.KEY, nextStep);

        // current phase no longer exists
        currentStep = new StepKey("phase_3", "action_2", "step_2");
        nextStep = policy.getNextValidStep(currentStep);
        assertNotNull(nextStep);
        assertEquals(new StepKey("phase_2", PhaseAfterStep.NAME, PhaseAfterStep.NAME), nextStep);

        // current phase no longer exists and was last phase
        currentStep = new StepKey("phase_5", "action_2", "step_2");
        nextStep = policy.getNextValidStep(currentStep);
        assertNotNull(nextStep);
        assertEquals(TerminalPolicyStep.KEY, nextStep);

        // create a new policy where only phase 2 exists and within it has
        // actions 1 and 3 which both contain steps 1, 2, and 3.
        phases = new HashMap<>();
        String phaseName = "phase_2";
        Map<String, LifecycleAction> actions = new HashMap<>();

        for (int a = 1; a <= 3; a++) {
            if (a == 2) {
                continue;
            }
            String actionName = "action_" + a;
            List<Step> steps = new ArrayList<>();
            for (int s = 1; s <= 3; s++) {
                String stepName = "step_" + s;
                steps.add(new MockStep(new StepKey(phaseName, actionName, stepName), null));
            }
            NamedMockAction action = new NamedMockAction(actionName, steps);
            actions.put(action.getWriteableName(), action);
        }

        Phase phase = new Phase(phaseName, TimeValue.ZERO, actions);
        phases.put(phase.getName(), phase);
        policy = new LifecyclePolicy(lifecycleType, lifecycleName, phases);

        // current phase no longer exists and was first phase
        currentStep = new StepKey("phase_1", "action_2", "step_2");
        nextStep = policy.getNextValidStep(currentStep);
        assertNotNull(nextStep);
        assertEquals(InitializePolicyContextStep.KEY, nextStep);

    }

    private static class NamedMockAction extends MockAction {

        private final String name;

        NamedMockAction(String name, List<Step> steps) {
            super(steps, true);
            this.name = name;
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    private static class ControllableLifecycleType implements LifecycleType {

        private final List<String> orderedPhaseNames;
        private final Map<String, List<String>> orderedActionNamesForPhases;

        ControllableLifecycleType(List<String> orderedPhases, Map<String, List<String>> orderedActionNamesForPhases) {
            this.orderedPhaseNames = orderedPhases;
            this.orderedActionNamesForPhases = orderedActionNamesForPhases;
        }

        @Override
        public String getWriteableName() {
            return "controllable_lifecycle_type";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public List<Phase> getOrderedPhases(Map<String, Phase> phases) {
            return orderedPhaseNames.stream().map(n -> phases.get(n)).filter(Objects::nonNull).collect(Collectors.toList());
        }

        @Override
        public String getNextPhaseName(String currentPhaseName, Map<String, Phase> phases) {
            int index = orderedPhaseNames.indexOf(currentPhaseName);
            if (index < 0) {
                throw new IllegalArgumentException(
                        "[" + currentPhaseName + "] is not a valid phase for lifecycle type [" + getWriteableName() + "]");
            } else if (index == orderedPhaseNames.size() - 1) {
                return null;
            } else {
             // Find the next phase after `index` that exists in `phases` and return it
                while (++index < orderedPhaseNames.size()) {
                    String phaseName = orderedPhaseNames.get(index);
                    if (phases.containsKey(phaseName)) {
                        return phaseName;
                    }
                }
                // if we have exhausted VALID_PHASES and haven't found a matching
                // phase in `phases` return null indicating there is no next phase
                // available
                return null;
            }
        }

        @Override
        public String getPreviousPhaseName(String currentPhaseName, Map<String, Phase> phases) {
            int index = orderedPhaseNames.indexOf(currentPhaseName);
            if (index < 0) {
                throw new IllegalArgumentException(
                        "[" + currentPhaseName + "] is not a valid phase for lifecycle type [" + getWriteableName() + "]");
            } else if (index == orderedPhaseNames.size() - 1) {
                return null;
            } else {
             // Find the previous phase before `index` that exists in `phases` and return it
                while (--index >= 0) {
                    String phaseName = orderedPhaseNames.get(index);
                    if (phases.containsKey(phaseName)) {
                        return phaseName;
                    }
                }
                // if we have exhausted VALID_PHASES and haven't found a matching
                // phase in `phases` return null indicating there is no next phase
                // available
                return null;
            }
        }

        @Override
        public List<LifecycleAction> getOrderedActions(Phase phase) {
            List<String> orderedActionNames = orderedActionNamesForPhases.get(phase.getName());
            if (orderedActionNames == null) {
                throw new IllegalArgumentException(
                        "[" + phase.getName() + "] is not a valid phase for lifecycle type [" + getWriteableName() + "]");
            }

            return orderedActionNames.stream().map(n -> phase.getActions().get(n)).filter(Objects::nonNull).collect(Collectors.toList());
        }

        @Override
        public String getNextActionName(String currentActionName, Phase phase) {
            List<String> orderedActionNames = orderedActionNamesForPhases.get(phase.getName());
            if (orderedActionNames == null) {
                throw new IllegalArgumentException(
                        "[" + phase.getName() + "] is not a valid phase for lifecycle type [" + getWriteableName() + "]");
            }

            int index = orderedActionNames.indexOf(currentActionName);
            if (index < 0) {
                throw new IllegalArgumentException("[" + currentActionName + "] is not a valid action for phase [" + phase.getName()
                        + "] in lifecycle type [" + getWriteableName() + "]");
            } else {
                // Find the next action after `index` that exists in the phase and return it
                while (++index < orderedActionNames.size()) {
                    String actionName = orderedActionNames.get(index);
                    if (phase.getActions().containsKey(actionName)) {
                        return actionName;
                    }
                }
                // if we have exhausted `validActions` and haven't found a matching
                // action in the Phase return null indicating there is no next
                // action available
                return null;
            }
        }

        @Override
        public void validate(Collection<Phase> phases) {
            phases.forEach(phase -> {
                if (orderedPhaseNames.contains(phase.getName()) == false) {
                    throw new IllegalArgumentException("Timeseries lifecycle does not support phase [" + phase.getName() + "]");
                }
                List<String> allowedActions = orderedActionNamesForPhases.get(phase.getName());
                phase.getActions().forEach((actionName, action) -> {
                    if (allowedActions.contains(actionName) == false) {
                        throw new IllegalArgumentException(
                                "invalid action [" + actionName + "] " + "defined in phase [" + phase.getName() + "]");
                    }
                });
            });
        }
    }
}
