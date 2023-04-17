/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class LifecyclePolicyTests extends AbstractXContentSerializingTestCase<LifecyclePolicy> {

    private String lifecycleName;

    @Override
    protected LifecyclePolicy doParseInstance(XContentParser parser) {
        return LifecyclePolicy.parse(parser, lifecycleName);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(
                    LifecycleType.class,
                    TimeseriesLifecycleType.TYPE,
                    (in) -> TimeseriesLifecycleType.INSTANCE
                ),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, AllocateAction.NAME, AllocateAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, WaitForSnapshotAction.NAME, WaitForSnapshotAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::readFrom),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ReadOnlyAction.NAME, ReadOnlyAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::read),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, FreezeAction.NAME, in -> FreezeAction.INSTANCE),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, SetPriorityAction.NAME, SetPriorityAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, UnfollowAction.NAME, in -> UnfollowAction.INSTANCE),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, MigrateAction.NAME, MigrateAction::readFrom),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, SearchableSnapshotAction.NAME, SearchableSnapshotAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, DownsampleAction.NAME, DownsampleAction::new)
            )
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(
            Arrays.asList(
                new NamedXContentRegistry.Entry(
                    LifecycleType.class,
                    new ParseField(TimeseriesLifecycleType.TYPE),
                    (p) -> TimeseriesLifecycleType.INSTANCE
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(WaitForSnapshotAction.NAME),
                    WaitForSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MigrateAction.NAME), MigrateAction::parse),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(SearchableSnapshotAction.NAME),
                    SearchableSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DownsampleAction.NAME), DownsampleAction::parse)
            )
        );
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
        List<String> phaseNames = TimeseriesLifecycleType.ORDERED_VALID_PHASES;
        Map<String, Phase> phases = Maps.newMapWithExpectedSize(phaseNames.size());
        Function<String, Set<String>> validActions = getPhaseToValidActions();
        Function<String, LifecycleAction> randomAction = getNameToActionFunction();
        TimeValue prev = null;
        for (String phase : phaseNames) {
            TimeValue after = prev == null
                ? TimeValue.parseTimeValue(randomTimeValue(0, 100000, "s", "m", "h", "d"), "test_after")
                : TimeValue.timeValueSeconds(prev.seconds() + randomIntBetween(60, 600));
            prev = after;
            Map<String, LifecycleAction> actions = new HashMap<>();
            Set<String> actionNames = validActions.apply(phase);
            if (phase.equals(TimeseriesLifecycleType.HOT_PHASE) == false) {
                // let's make sure the other phases don't configure actions that conflict with the `searchable_snapshot` action
                // configured in the hot phase
                actionNames.removeAll(TimeseriesLifecycleType.ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT);
            }
            for (String action : actionNames) {
                actions.put(action, randomAction.apply(action));
            }
            phases.put(phase, new Phase(phase, after, actions));
        }
        return new LifecyclePolicy(TimeseriesLifecycleType.INSTANCE, lifecycleName, phases, randomMeta());
    }

    public static LifecyclePolicy randomTimeseriesLifecyclePolicy(@Nullable String lifecycleName) {
        List<String> phaseNames = randomSubsetOf(
            between(0, TimeseriesLifecycleType.ORDERED_VALID_PHASES.size() - 1),
            TimeseriesLifecycleType.ORDERED_VALID_PHASES
        ).stream()
            // Remove the frozen phase, we'll randomly re-add it later
            .filter(pn -> TimeseriesLifecycleType.FROZEN_PHASE.equals(pn) == false)
            .collect(Collectors.toList());
        Map<String, Phase> phases = Maps.newMapWithExpectedSize(phaseNames.size());
        Function<String, Set<String>> validActions = getPhaseToValidActions();
        Function<String, LifecycleAction> randomAction = getNameToActionFunction();
        // as what actions end up in the hot phase influence what actions are allowed in the subsequent phases we'll move the hot phase
        // at the front of the phases to process (if it exists)
        if (phaseNames.contains(TimeseriesLifecycleType.HOT_PHASE)) {
            phaseNames.remove(TimeseriesLifecycleType.HOT_PHASE);
            phaseNames.add(0, TimeseriesLifecycleType.HOT_PHASE);
        }
        boolean hotPhaseContainsSearchableSnap = false;
        boolean coldPhaseContainsSearchableSnap = false;
        // let's order the phases so we can reason about actions in a previous phase in order to generate a random *valid* policy
        List<String> orderedPhases = new ArrayList<>(phaseNames.size());
        for (String validPhase : TimeseriesLifecycleType.ORDERED_VALID_PHASES) {
            if (phaseNames.contains(validPhase)) {
                orderedPhases.add(validPhase);
            }
        }

        TimeValue prev = null;
        for (String phase : orderedPhases) {
            TimeValue after = prev == null
                ? TimeValue.parseTimeValue(randomTimeValue(0, 100000, "s", "m", "h", "d"), "test_after")
                : TimeValue.timeValueSeconds(prev.seconds() + randomIntBetween(60, 600));
            prev = after;
            Map<String, LifecycleAction> actions = new HashMap<>();
            List<String> actionNames = randomSubsetOf(validActions.apply(phase));

            if (phase.equals(TimeseriesLifecycleType.HOT_PHASE)) {
                // If the hot phase has any actions that require a rollover, then ensure there is one so that the policy will validate
                if (actionNames.stream().anyMatch(TimeseriesLifecycleType.HOT_ACTIONS_THAT_REQUIRE_ROLLOVER::contains)) {
                    actionNames.add(RolloverAction.NAME);
                }

                if (actionNames.contains(SearchableSnapshotAction.NAME)) {
                    hotPhaseContainsSearchableSnap = true;
                }
            }
            if (phase.equals(TimeseriesLifecycleType.COLD_PHASE)) {
                if (hotPhaseContainsSearchableSnap) {
                    // let's make sure the other phases don't configure actions that conflict with a possible `searchable_snapshot` action
                    // configured in the hot phase
                    actionNames.removeAll(TimeseriesLifecycleType.ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT);
                }

                if (actionNames.contains(SearchableSnapshotAction.NAME)) {
                    coldPhaseContainsSearchableSnap = true;
                }
            } else {
                if (hotPhaseContainsSearchableSnap || coldPhaseContainsSearchableSnap) {
                    // let's make sure the other phases don't configure actions that conflict with a possible `searchable_snapshot` action
                    // configured in a previous phase (hot/cold)
                    actionNames.removeAll(TimeseriesLifecycleType.ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT);
                }
            }

            for (String action : actionNames) {
                actions.put(action, randomAction.apply(action));
            }
            phases.put(phase, new Phase(phase, after, actions));
        }
        // Add a frozen phase if neither the hot nor cold phase contains a searchable snapshot action
        if (hotPhaseContainsSearchableSnap == false && coldPhaseContainsSearchableSnap == false && randomBoolean()) {
            TimeValue frozenTime = prev == null
                ? TimeValue.parseTimeValue(randomTimeValue(0, 100000, "s", "m", "h", "d"), "test")
                : TimeValue.timeValueSeconds(prev.seconds() + randomIntBetween(60, 600));
            phases.put(
                TimeseriesLifecycleType.FROZEN_PHASE,
                new Phase(
                    TimeseriesLifecycleType.FROZEN_PHASE,
                    frozenTime,
                    Collections.singletonMap(
                        SearchableSnapshotAction.NAME,
                        new SearchableSnapshotAction(randomAlphaOfLength(10), randomBoolean())
                    )
                )
            );
        } else {
            phases.remove(TimeseriesLifecycleType.FROZEN_PHASE);
        }
        return new LifecyclePolicy(TimeseriesLifecycleType.INSTANCE, lifecycleName, phases, randomMeta());
    }

    private static Function<String, Set<String>> getPhaseToValidActions() {
        return (phase) -> new HashSet<>(switch (phase) {
            case "hot" -> TimeseriesLifecycleType.VALID_HOT_ACTIONS;
            case "warm" -> TimeseriesLifecycleType.VALID_WARM_ACTIONS;
            case "cold" -> TimeseriesLifecycleType.VALID_COLD_ACTIONS;
            case "frozen" -> TimeseriesLifecycleType.VALID_FROZEN_ACTIONS;
            case "delete" -> TimeseriesLifecycleType.VALID_DELETE_ACTIONS;
            default -> throw new IllegalArgumentException("invalid phase [" + phase + "]");
        });
    }

    private static Function<String, LifecycleAction> getNameToActionFunction() {
        return (action) -> switch (action) {
            case AllocateAction.NAME -> AllocateActionTests.randomInstance();
            case WaitForSnapshotAction.NAME -> WaitForSnapshotActionTests.randomInstance();
            case DeleteAction.NAME -> DeleteAction.WITH_SNAPSHOT_DELETE;
            case ForceMergeAction.NAME -> ForceMergeActionTests.randomInstance();
            case ReadOnlyAction.NAME -> new ReadOnlyAction();
            case RolloverAction.NAME -> RolloverActionTests.randomInstance();
            case ShrinkAction.NAME -> ShrinkActionTests.randomInstance();
            case FreezeAction.NAME -> FreezeAction.INSTANCE;
            case SetPriorityAction.NAME -> SetPriorityActionTests.randomInstance();
            case UnfollowAction.NAME -> UnfollowAction.INSTANCE;
            case SearchableSnapshotAction.NAME -> new SearchableSnapshotAction("repo", randomBoolean());
            case MigrateAction.NAME -> MigrateAction.DISABLED;
            case DownsampleAction.NAME -> DownsampleActionTests.randomInstance();
            default -> throw new IllegalArgumentException("invalid action [" + action + "]");
        };
    }

    public static LifecyclePolicy randomTestLifecyclePolicy(@Nullable String lifecycleName) {
        int numberPhases = randomInt(5);
        Map<String, Phase> phases = Maps.newMapWithExpectedSize(numberPhases);
        for (int i = 0; i < numberPhases; i++) {
            TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 10000, "s", "m", "h", "d"), "test_after");
            Map<String, LifecycleAction> actions = new HashMap<>();
            if (randomBoolean()) {
                MockAction action = new MockAction();
                actions.put(action.getWriteableName(), action);
            }
            String phaseName = randomAlphaOfLength(10);
            phases.put(phaseName, new Phase(phaseName, after, actions));
        }
        return new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases, randomMeta());
    }

    @Override
    protected LifecyclePolicy mutateInstance(LifecyclePolicy instance) {
        String name = instance.getName();
        Map<String, Phase> phases = instance.getPhases();
        switch (between(0, 1)) {
            case 0 -> name = name + randomAlphaOfLengthBetween(1, 5);
            case 1 -> {
                // Remove the frozen phase, because it makes a lot of invalid phases when randomly mutating an existing policy
                phases.remove(TimeseriesLifecycleType.FROZEN_PHASE);
                // Remove a random phase
                if (phases.size() > 0) {
                    phases.remove(new ArrayList<>(phases.keySet()).remove(randomIntBetween(0, phases.size() - 1)));
                }
                String phaseName = randomValueOtherThanMany(
                    phases::containsKey,
                    () -> randomFrom(
                        TimeseriesLifecycleType.ORDERED_VALID_PHASES.stream()
                            .filter(pn -> TimeseriesLifecycleType.FROZEN_PHASE.equals(pn) == false)
                            .collect(Collectors.toList())
                    )
                );
                phases = new LinkedHashMap<>(phases);
                phases.put(phaseName, new Phase(phaseName, null, Collections.emptyMap()));
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new LifecyclePolicy(TimeseriesLifecycleType.INSTANCE, name, phases, randomMeta());
    }

    @Override
    protected Reader<LifecyclePolicy> instanceReader() {
        return LifecyclePolicy::new;
    }

    public void testFirstAndLastSteps() {
        Client client = mock(Client.class);
        lifecycleName = randomAlphaOfLengthBetween(1, 20);
        Map<String, Phase> phases = new LinkedHashMap<>();
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases, randomMeta());
        List<Step> steps = policy.toSteps(client, null);
        assertThat(steps.size(), equalTo(2));
        assertThat(steps.get(0), instanceOf(InitializePolicyContextStep.class));
        assertThat(steps.get(0).getKey(), equalTo(new StepKey("new", "init", "init")));
        assertThat(steps.get(0).getNextStepKey(), equalTo(PhaseCompleteStep.finalStep("new").getKey()));
        assertThat(steps.get(1), equalTo(PhaseCompleteStep.finalStep("new")));
    }

    public void testToStepsWithOneStep() {
        Client client = mock(Client.class);
        MockStep mockStep = new MockStep(new Step.StepKey("test", "test", "test"), PhaseCompleteStep.finalStep("test").getKey());

        lifecycleName = randomAlphaOfLengthBetween(1, 20);
        Map<String, Phase> phases = new LinkedHashMap<>();
        LifecycleAction firstAction = new MockAction(Arrays.asList(mockStep));
        Map<String, LifecycleAction> actions = Collections.singletonMap(MockAction.NAME, firstAction);
        Phase firstPhase = new Phase("test", TimeValue.ZERO, actions);
        phases.put(firstPhase.getName(), firstPhase);
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases, randomMeta());
        StepKey firstStepKey = InitializePolicyContextStep.KEY;
        StepKey secondStepKey = PhaseCompleteStep.finalStep("new").getKey();
        List<Step> steps = policy.toSteps(client, null);
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
        MockStep secondActionStep = new MockStep(
            new StepKey("second_phase", "test2", "test"),
            PhaseCompleteStep.finalStep("second_phase").getKey()
        );
        MockStep secondAfter = new MockStep(
            new StepKey("first_phase", PhaseCompleteStep.NAME, PhaseCompleteStep.NAME),
            secondActionStep.getKey()
        );
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
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases, randomMeta());

        List<Step> steps = policy.toSteps(client, null);
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
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, phases, randomMeta());

        assertTrue(policy.isActionSafe(new StepKey("first_phase", MockAction.NAME, randomAlphaOfLength(10))));

        assertFalse(policy.isActionSafe(new StepKey("second_phase", MockAction.NAME, randomAlphaOfLength(10))));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> policy.isActionSafe(new StepKey("non_existant_phase", MockAction.NAME, randomAlphaOfLength(10)))
        );
        assertEquals("Phase [non_existant_phase]  does not exist in policy [" + policy.getName() + "]", exception.getMessage());

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> policy.isActionSafe(new StepKey("first_phase", "non_existant_action", randomAlphaOfLength(10)))
        );
        assertEquals(
            "Action [non_existant_action] in phase [first_phase]  does not exist in policy [" + policy.getName() + "]",
            exception.getMessage()
        );

        assertTrue(policy.isActionSafe(new StepKey("new", randomAlphaOfLength(10), randomAlphaOfLength(10))));
    }

    public void testValidatePolicyName() {
        expectThrows(IllegalArgumentException.class, () -> LifecyclePolicy.validatePolicyName(null));
        expectThrows(IllegalArgumentException.class, () -> LifecyclePolicy.validatePolicyName(""));
        expectThrows(
            IllegalArgumentException.class,
            () -> LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(0, 10) + "," + randomAlphaOfLengthBetween(0, 10))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(0, 10) + " " + randomAlphaOfLengthBetween(0, 10))
        );
        expectThrows(IllegalArgumentException.class, () -> LifecyclePolicy.validatePolicyName("_" + randomAlphaOfLengthBetween(1, 20)));
        expectThrows(IllegalArgumentException.class, () -> LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(256, 1000)));

        LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(1, 10) + "_" + randomAlphaOfLengthBetween(0, 10));

        LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(0, 10) + "-" + randomAlphaOfLengthBetween(0, 10));
        LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(0, 10) + "+" + randomAlphaOfLengthBetween(0, 10));

        LifecyclePolicy.validatePolicyName(randomAlphaOfLengthBetween(1, 255));
    }

    public static Map<String, Object> randomMeta() {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(4));
            } else {
                return Collections.singletonMap(
                    randomAlphaOfLength(5),
                    Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(4))
                );
            }
        } else {
            return null;
        }
    }
}
