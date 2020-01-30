/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_COLD_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_DELETE_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_HOT_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_WARM_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_COLD_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_DELETE_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_HOT_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_PHASES;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_WARM_ACTIONS;
import static org.hamcrest.Matchers.equalTo;

public class TimeseriesLifecycleTypeTests extends ESTestCase {

    private static final AllocateAction TEST_ALLOCATE_ACTION =
        new AllocateAction(2, Collections.singletonMap("node", "node1"),null, null);
    private static final DeleteAction TEST_DELETE_ACTION = new DeleteAction();
    private static final WaitForSnapshotAction TEST_WAIT_FOR_SNAPSHOT_ACTION = new WaitForSnapshotAction("policy");
    private static final ForceMergeAction TEST_FORCE_MERGE_ACTION = new ForceMergeAction(1);
    private static final RolloverAction TEST_ROLLOVER_ACTION = new RolloverAction(new ByteSizeValue(1), null, null);
    private static final ShrinkAction TEST_SHRINK_ACTION = new ShrinkAction(1);
    private static final ReadOnlyAction TEST_READ_ONLY_ACTION = new ReadOnlyAction();
    private static final FreezeAction TEST_FREEZE_ACTION = new FreezeAction();
    private static final SetPriorityAction TEST_PRIORITY_ACTION = new SetPriorityAction(0);
    private static final UnfollowAction TEST_UNFOLLOW_ACTION  = new UnfollowAction();

    public void testValidatePhases() {
        boolean invalid = randomBoolean();
        String phaseName = randomFrom("hot", "warm", "cold", "delete");
        if (invalid) {
            phaseName += randomAlphaOfLength(5);
        }
        Map<String, Phase> phases = Collections.singletonMap(phaseName,
            new Phase(phaseName, TimeValue.ZERO, Collections.emptyMap()));
        if (invalid) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> TimeseriesLifecycleType.INSTANCE.validate(phases.values()));
            assertThat(e.getMessage(), equalTo("Timeseries lifecycle does not support phase [" + phaseName + "]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(phases.values());
        }
    }

    public void testValidateHotPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = VALID_HOT_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "forcemerge", "delete", "shrink", "freeze"));
            actions.put(invalidAction.getWriteableName(), invalidAction);
        }
        Map<String, Phase> hotPhase = Collections.singletonMap("hot",
            new Phase("hot", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class,
                    () -> TimeseriesLifecycleType.INSTANCE.validate(hotPhase.values()));
            assertThat(e.getMessage(),
                equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [hot]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(hotPhase.values());
        }
    }

    public void testValidateWarmPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_WARM_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete", "freeze"));
            actions.put(invalidAction.getWriteableName(), invalidAction);
        }
        Map<String, Phase> warmPhase = Collections.singletonMap("warm",
            new Phase("warm", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class,
                    () -> TimeseriesLifecycleType.INSTANCE.validate(warmPhase.values()));
            assertThat(e.getMessage(),
            equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [warm]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(warmPhase.values());
        }
    }

    public void testValidateColdPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_COLD_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete", "forcemerge", "shrink"));
            actions.put(invalidAction.getWriteableName(), invalidAction);
        }
        Map<String, Phase> coldPhase = Collections.singletonMap("cold",
            new Phase("cold", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class,
                    () -> TimeseriesLifecycleType.INSTANCE.validate(coldPhase.values()));
            assertThat(e.getMessage(),
                equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [cold]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(coldPhase.values());
        }
    }

    public void testValidateDeletePhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = VALID_DELETE_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "rollover", "forcemerge", "shrink", "freeze", "set_priority"));
            actions.put(invalidAction.getWriteableName(), invalidAction);
        }
        Map<String, Phase> deletePhase = Collections.singletonMap("delete",
            new Phase("delete", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class,
                    () -> TimeseriesLifecycleType.INSTANCE.validate(deletePhase.values()));
            assertThat(e.getMessage(),
                equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [delete]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(deletePhase.values());
        }
    }

    public void testGetOrderedPhases() {
        Map<String, Phase> phaseMap = new HashMap<>();
        for (String phaseName : randomSubsetOf(randomIntBetween(0, VALID_PHASES.size()), VALID_PHASES)) {
            phaseMap.put(phaseName, new Phase(phaseName, TimeValue.ZERO, Collections.emptyMap()));
        }


        assertTrue(isSorted(TimeseriesLifecycleType.INSTANCE.getOrderedPhases(phaseMap), Phase::getName, VALID_PHASES));
    }

    public void testUnfollowInjections() {
        assertTrue(isUnfollowInjected("hot", RolloverAction.NAME));
        assertTrue(isUnfollowInjected("warm", ShrinkAction.NAME));

        assertFalse(isUnfollowInjected("hot", SetPriorityAction.NAME));
        assertFalse(isUnfollowInjected("warm", SetPriorityAction.NAME));
        assertFalse(isUnfollowInjected("warm", AllocateAction.NAME));
        assertFalse(isUnfollowInjected("warm", ReadOnlyAction.NAME));
        assertFalse(isUnfollowInjected("warm", ForceMergeAction.NAME));
        assertFalse(isUnfollowInjected("cold", SetPriorityAction.NAME));
        assertFalse(isUnfollowInjected("cold", AllocateAction.NAME));
        assertFalse(isUnfollowInjected("cold", FreezeAction.NAME));
        assertFalse(isUnfollowInjected("delete", DeleteAction.NAME));

    }

    private boolean isUnfollowInjected(String phaseName, String actionName) {
        Map<String, Phase> phaseMap = new HashMap<>();
        Map<String, LifecycleAction> actionsMap = new HashMap<>();
        actionsMap.put(actionName, getTestAction(actionName));
        Phase warmPhase = new Phase(phaseName, TimeValue.ZERO, actionsMap);
        phaseMap.put(phaseName, warmPhase);
        List<Phase> phases = TimeseriesLifecycleType.INSTANCE.getOrderedPhases(phaseMap);
        Phase processedWarmPhase = phases.stream().filter(phase -> phase.getName().equals(phaseName)).findFirst().get();
        return processedWarmPhase.getActions().containsKey("unfollow");
    }

    public void testGetOrderedActionsInvalidPhase() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TimeseriesLifecycleType.INSTANCE
            .getOrderedActions(new Phase("invalid", TimeValue.ZERO, Collections.emptyMap())));
        assertThat(exception.getMessage(), equalTo("lifecycle type[timeseries] does not support phase[invalid]"));
    }

    public void testGetOrderedActionsHot() {
        Map<String, LifecycleAction> actions = VALID_HOT_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(hotPhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_HOT_ACTIONS));
        assertThat(orderedActions.indexOf(TEST_PRIORITY_ACTION), equalTo(0));
    }

    public void testGetOrderedActionsWarm() {
        Map<String, LifecycleAction> actions = VALID_WARM_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(warmPhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_WARM_ACTIONS));
        assertThat(orderedActions.indexOf(TEST_PRIORITY_ACTION), equalTo(0));
    }

    public void testGetOrderedActionsCold() {
        Map<String, LifecycleAction> actions = VALID_COLD_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase coldPhase = new Phase("cold", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(coldPhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_COLD_ACTIONS));
        assertThat(orderedActions.indexOf(TEST_PRIORITY_ACTION), equalTo(0));
    }

    public void testGetOrderedActionsDelete() {
        Map<String, LifecycleAction> actions = VALID_DELETE_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase deletePhase = new Phase("delete", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(deletePhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_DELETE_ACTIONS));
    }

    public void testGetNextPhaseName() {
        assertNextPhaseName("hot", "warm", new String[] { "hot", "warm" });
        assertNextPhaseName("hot", "warm", new String[] { "hot", "warm", "cold" });
        assertNextPhaseName("hot", "warm", new String[] { "hot", "warm", "cold", "delete" });
        assertNextPhaseName("hot", "warm", new String[] { "warm", "cold", "delete" });
        assertNextPhaseName("hot", "warm", new String[] { "warm", "cold", "delete" });
        assertNextPhaseName("hot", "warm", new String[] { "warm", "delete" });
        assertNextPhaseName("hot", "cold", new String[] { "cold", "delete" });
        assertNextPhaseName("hot", "cold", new String[] { "cold" });
        assertNextPhaseName("hot", "delete", new String[] { "hot", "delete" });
        assertNextPhaseName("hot", "delete", new String[] { "delete" });
        assertNextPhaseName("hot", null, new String[] { "hot" });
        assertNextPhaseName("hot", null, new String[] {});

        assertNextPhaseName("warm", "cold", new String[] { "hot", "warm", "cold", "delete" });
        assertNextPhaseName("warm", "cold", new String[] { "warm", "cold", "delete" });
        assertNextPhaseName("warm", "cold", new String[] { "cold", "delete" });
        assertNextPhaseName("warm", "cold", new String[] { "cold" });
        assertNextPhaseName("warm", "delete", new String[] { "hot", "warm", "delete" });
        assertNextPhaseName("warm", null, new String[] { "hot", "warm" });
        assertNextPhaseName("warm", null, new String[] { "warm" });
        assertNextPhaseName("warm", null, new String[] { "hot" });
        assertNextPhaseName("warm", null, new String[] {});

        assertNextPhaseName("cold", "delete", new String[] { "hot", "warm", "cold", "delete" });
        assertNextPhaseName("cold", "delete", new String[] { "warm", "cold", "delete" });
        assertNextPhaseName("cold", "delete", new String[] { "cold", "delete" });
        assertNextPhaseName("cold", "delete", new String[] { "delete" });
        assertNextPhaseName("cold", "delete", new String[] { "hot", "warm", "delete" });
        assertNextPhaseName("cold", null, new String[] { "hot", "warm", "cold" });
        assertNextPhaseName("cold", null, new String[] { "hot", "warm" });
        assertNextPhaseName("cold", null, new String[] { "cold" });
        assertNextPhaseName("cold", null, new String[] { "hot" });
        assertNextPhaseName("cold", null, new String[] {});

        assertNextPhaseName("delete", null, new String[] { "hot", "warm", "cold" });
        assertNextPhaseName("delete", null, new String[] { "hot", "warm" });
        assertNextPhaseName("delete", null, new String[] { "cold" });
        assertNextPhaseName("delete", null, new String[] { "hot" });
        assertNextPhaseName("delete", null, new String[] {});
        assertNextPhaseName("delete", null, new String[] { "hot", "warm", "cold", "delete" });
        assertNextPhaseName("delete", null, new String[] { "hot", "warm", "delete" });
        assertNextPhaseName("delete", null, new String[] { "cold", "delete" });
        assertNextPhaseName("delete", null, new String[] { "delete" });
        assertNextPhaseName("delete", null, new String[] {});

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.INSTANCE.getNextPhaseName("foo", Collections.emptyMap()));
        assertEquals("[foo] is not a valid phase for lifecycle type [" + TimeseriesLifecycleType.TYPE + "]", exception.getMessage());
        exception = expectThrows(IllegalArgumentException.class, () -> TimeseriesLifecycleType.INSTANCE
                .getNextPhaseName("foo", Collections.singletonMap("foo", new Phase("foo", TimeValue.ZERO, Collections.emptyMap()))));
        assertEquals("[foo] is not a valid phase for lifecycle type [" + TimeseriesLifecycleType.TYPE + "]", exception.getMessage());
    }

    public void testGetPreviousPhaseName() {
        assertPreviousPhaseName("hot", null, new String[] { "hot", "warm" });
        assertPreviousPhaseName("hot", null, new String[] { "hot", "warm", "cold" });
        assertPreviousPhaseName("hot", null, new String[] { "hot", "warm", "cold", "delete" });
        assertPreviousPhaseName("hot", null, new String[] { "warm", "cold", "delete" });
        assertPreviousPhaseName("hot", null, new String[] { "warm", "delete" });
        assertPreviousPhaseName("hot", null, new String[] { "cold", "delete" });
        assertPreviousPhaseName("hot", null, new String[] { "cold" });
        assertPreviousPhaseName("hot", null, new String[] { "hot", "delete" });
        assertPreviousPhaseName("hot", null, new String[] { "delete" });
        assertPreviousPhaseName("hot", null, new String[] { "hot" });
        assertPreviousPhaseName("hot", null, new String[] {});

        assertPreviousPhaseName("warm", "hot", new String[] { "hot", "warm", "cold", "delete" });
        assertPreviousPhaseName("warm", null, new String[] { "warm", "cold", "delete" });
        assertPreviousPhaseName("warm", "hot", new String[] { "hot", "cold", "delete" });
        assertPreviousPhaseName("warm", null, new String[] { "cold", "delete" });
        assertPreviousPhaseName("warm", "hot", new String[] { "hot", "delete" });
        assertPreviousPhaseName("warm", null, new String[] { "delete" });
        assertPreviousPhaseName("warm", "hot", new String[] { "hot" });
        assertPreviousPhaseName("warm", null, new String[] {});

        assertPreviousPhaseName("cold", "warm", new String[] { "hot", "warm", "cold", "delete" });
        assertPreviousPhaseName("cold", "hot", new String[] { "hot", "cold", "delete" });
        assertPreviousPhaseName("cold", "warm", new String[] { "warm", "cold", "delete" });
        assertPreviousPhaseName("cold", null, new String[] { "cold", "delete" });
        assertPreviousPhaseName("cold", "warm", new String[] { "hot", "warm", "delete" });
        assertPreviousPhaseName("cold", "hot", new String[] { "hot", "delete" });
        assertPreviousPhaseName("cold", "warm", new String[] { "warm", "delete" });
        assertPreviousPhaseName("cold", null, new String[] { "delete" });
        assertPreviousPhaseName("cold", "warm", new String[] { "hot", "warm" });
        assertPreviousPhaseName("cold", "hot", new String[] { "hot" });
        assertPreviousPhaseName("cold", "warm", new String[] { "warm" });
        assertPreviousPhaseName("cold", null, new String[] {});

        assertPreviousPhaseName("delete", "cold", new String[] { "hot", "warm", "cold", "delete" });
        assertPreviousPhaseName("delete", "cold", new String[] { "warm", "cold", "delete" });
        assertPreviousPhaseName("delete", "warm", new String[] { "hot", "warm", "delete" });
        assertPreviousPhaseName("delete", "hot", new String[] { "hot", "delete" });
        assertPreviousPhaseName("delete", "cold", new String[] { "cold", "delete" });
        assertPreviousPhaseName("delete", null, new String[] { "delete" });
        assertPreviousPhaseName("delete", "cold", new String[] { "hot", "warm", "cold" });
        assertPreviousPhaseName("delete", "cold", new String[] { "warm", "cold" });
        assertPreviousPhaseName("delete", "warm", new String[] { "hot", "warm" });
        assertPreviousPhaseName("delete", "hot", new String[] { "hot" });
        assertPreviousPhaseName("delete", "cold", new String[] { "cold" });
        assertPreviousPhaseName("delete", null, new String[] {});

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.INSTANCE.getPreviousPhaseName("foo", Collections.emptyMap()));
        assertEquals("[foo] is not a valid phase for lifecycle type [" + TimeseriesLifecycleType.TYPE + "]", exception.getMessage());
        exception = expectThrows(IllegalArgumentException.class, () -> TimeseriesLifecycleType.INSTANCE
                .getPreviousPhaseName("foo", Collections.singletonMap("foo", new Phase("foo", TimeValue.ZERO, Collections.emptyMap()))));
        assertEquals("[foo] is not a valid phase for lifecycle type [" + TimeseriesLifecycleType.TYPE + "]", exception.getMessage());
    }

    public void testGetNextActionName() {
        // Hot Phase
        assertNextActionName("hot", SetPriorityAction.NAME, UnfollowAction.NAME,
            new String[] {UnfollowAction.NAME, RolloverAction.NAME});
        assertNextActionName("hot", SetPriorityAction.NAME, RolloverAction.NAME, new String[]{RolloverAction.NAME});
        assertNextActionName("hot", SetPriorityAction.NAME, null, new String[] {});

        assertNextActionName("hot", RolloverAction.NAME, null, new String[] {});
        assertNextActionName("hot", RolloverAction.NAME, null, new String[] { RolloverAction.NAME });

        assertInvalidAction("hot", "foo", new String[] { RolloverAction.NAME });
        assertInvalidAction("hot", AllocateAction.NAME, new String[] { RolloverAction.NAME });
        assertInvalidAction("hot", DeleteAction.NAME, new String[] { RolloverAction.NAME });
        assertInvalidAction("hot", ForceMergeAction.NAME, new String[] { RolloverAction.NAME });
        assertInvalidAction("hot", ReadOnlyAction.NAME, new String[] { RolloverAction.NAME });
        assertInvalidAction("hot", ShrinkAction.NAME, new String[] { RolloverAction.NAME });

        // Warm Phase
        assertNextActionName("warm", SetPriorityAction.NAME, UnfollowAction.NAME,
            new String[]{SetPriorityAction.NAME, UnfollowAction.NAME, ReadOnlyAction.NAME, AllocateAction.NAME,
                ShrinkAction.NAME, ForceMergeAction.NAME});
        assertNextActionName("warm", SetPriorityAction.NAME, ReadOnlyAction.NAME,
            new String[]{SetPriorityAction.NAME, ReadOnlyAction.NAME, AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME});
        assertNextActionName("warm", SetPriorityAction.NAME, AllocateAction.NAME,
            new String[]{SetPriorityAction.NAME, AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME});
        assertNextActionName("warm", SetPriorityAction.NAME, ShrinkAction.NAME,
            new String[]{SetPriorityAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME});
        assertNextActionName("warm", SetPriorityAction.NAME, ForceMergeAction.NAME,
            new String[]{SetPriorityAction.NAME, ForceMergeAction.NAME});
        assertNextActionName("warm", SetPriorityAction.NAME, null, new String[]{SetPriorityAction.NAME});

        assertNextActionName("warm", UnfollowAction.NAME, ReadOnlyAction.NAME,
            new String[] { SetPriorityAction.NAME, ReadOnlyAction.NAME, AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", UnfollowAction.NAME, ReadOnlyAction.NAME,
                new String[] { ReadOnlyAction.NAME, AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", UnfollowAction.NAME, AllocateAction.NAME,
            new String[] { AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", UnfollowAction.NAME, ShrinkAction.NAME,
            new String[] { ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", UnfollowAction.NAME, ForceMergeAction.NAME, new String[] { ForceMergeAction.NAME });
        assertNextActionName("warm", UnfollowAction.NAME, null, new String[] {});

        assertNextActionName("warm", ReadOnlyAction.NAME, AllocateAction.NAME,
                new String[] { ReadOnlyAction.NAME, AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", ReadOnlyAction.NAME, ShrinkAction.NAME,
                new String[] { ReadOnlyAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", ReadOnlyAction.NAME, ForceMergeAction.NAME,
                new String[] { ReadOnlyAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", ReadOnlyAction.NAME, null, new String[] { ReadOnlyAction.NAME });

        assertNextActionName("warm", ReadOnlyAction.NAME, AllocateAction.NAME,
                new String[] { AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", ReadOnlyAction.NAME, ShrinkAction.NAME, new String[] { ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", ReadOnlyAction.NAME, ForceMergeAction.NAME, new String[] { ForceMergeAction.NAME });
        assertNextActionName("warm", ReadOnlyAction.NAME, null, new String[] {});

        assertNextActionName("warm", AllocateAction.NAME, ShrinkAction.NAME,
                new String[] { ReadOnlyAction.NAME, AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", AllocateAction.NAME, ForceMergeAction.NAME,
                new String[] { ReadOnlyAction.NAME, AllocateAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", AllocateAction.NAME, null, new String[] { ReadOnlyAction.NAME, AllocateAction.NAME });

        assertNextActionName("warm", AllocateAction.NAME, ShrinkAction.NAME, new String[] { ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", AllocateAction.NAME, ForceMergeAction.NAME, new String[] { ForceMergeAction.NAME });
        assertNextActionName("warm", AllocateAction.NAME, null, new String[] {});

        assertNextActionName("warm", ShrinkAction.NAME, ForceMergeAction.NAME,
                new String[] { ReadOnlyAction.NAME, AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });
        assertNextActionName("warm", ShrinkAction.NAME, null,
                new String[] { ReadOnlyAction.NAME, AllocateAction.NAME, ShrinkAction.NAME });

        assertNextActionName("warm", ShrinkAction.NAME, ForceMergeAction.NAME, new String[] { ForceMergeAction.NAME });
        assertNextActionName("warm", ShrinkAction.NAME, null, new String[] {});

        assertNextActionName("warm", ForceMergeAction.NAME, null,
                new String[] { ReadOnlyAction.NAME, AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });

        assertNextActionName("warm", ForceMergeAction.NAME, null, new String[] {});

        assertInvalidAction("warm", "foo", new String[] { RolloverAction.NAME });
        assertInvalidAction("warm", DeleteAction.NAME,
                new String[] { ReadOnlyAction.NAME, AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });
        assertInvalidAction("warm", RolloverAction.NAME,
                new String[] { ReadOnlyAction.NAME, AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME });

        // Cold Phase
        assertNextActionName("cold", SetPriorityAction.NAME, UnfollowAction.NAME,
            new String[]{UnfollowAction.NAME, SetPriorityAction.NAME, FreezeAction.NAME});
        assertNextActionName("cold", SetPriorityAction.NAME, FreezeAction.NAME,
            new String[]{SetPriorityAction.NAME, FreezeAction.NAME});
        assertNextActionName("cold", SetPriorityAction.NAME, AllocateAction.NAME,
            new String[]{SetPriorityAction.NAME, AllocateAction.NAME});
        assertNextActionName("cold", SetPriorityAction.NAME, null, new String[] { SetPriorityAction.NAME });
        assertNextActionName("cold", SetPriorityAction.NAME, null, new String[] {});

        assertNextActionName("cold", UnfollowAction.NAME, AllocateAction.NAME,
            new String[] {SetPriorityAction.NAME, AllocateAction.NAME, FreezeAction.NAME});
        assertNextActionName("cold", UnfollowAction.NAME, AllocateAction.NAME,
            new String[] {AllocateAction.NAME, FreezeAction.NAME});
        assertNextActionName("cold", UnfollowAction.NAME, FreezeAction.NAME, new String[] {FreezeAction.NAME});
        assertNextActionName("cold", UnfollowAction.NAME, null, new String[] {});

        assertNextActionName("cold", AllocateAction.NAME, null, new String[] { AllocateAction.NAME });
        assertNextActionName("cold", AllocateAction.NAME, null, new String[] {});
        assertNextActionName("cold", AllocateAction.NAME, null, new String[] {});
        assertNextActionName("cold", AllocateAction.NAME, FreezeAction.NAME, FreezeAction.NAME);

        assertNextActionName("cold", FreezeAction.NAME, null);
        assertNextActionName("cold", FreezeAction.NAME, null, AllocateAction.NAME);

        assertInvalidAction("cold", "foo", new String[] { AllocateAction.NAME });
        assertInvalidAction("cold", DeleteAction.NAME, new String[] { AllocateAction.NAME });
        assertInvalidAction("cold", ForceMergeAction.NAME, new String[] { AllocateAction.NAME });
        assertInvalidAction("cold", ReadOnlyAction.NAME, new String[] { AllocateAction.NAME });
        assertInvalidAction("cold", RolloverAction.NAME, new String[] { AllocateAction.NAME });
        assertInvalidAction("cold", ShrinkAction.NAME, new String[] { AllocateAction.NAME });

        // Delete Phase
        assertNextActionName("delete", DeleteAction.NAME, null, new String[] {});
        assertNextActionName("delete", DeleteAction.NAME, null, new String[] { DeleteAction.NAME });

        assertInvalidAction("delete", "foo", new String[] { DeleteAction.NAME });
        assertInvalidAction("delete", AllocateAction.NAME, new String[] { DeleteAction.NAME });
        assertInvalidAction("delete", ForceMergeAction.NAME, new String[] { DeleteAction.NAME });
        assertInvalidAction("delete", ReadOnlyAction.NAME, new String[] { DeleteAction.NAME });
        assertInvalidAction("delete", RolloverAction.NAME, new String[] { DeleteAction.NAME });
        assertInvalidAction("delete", ShrinkAction.NAME, new String[] { DeleteAction.NAME });
        assertInvalidAction("delete", FreezeAction.NAME, new String[] { DeleteAction.NAME });
        assertInvalidAction("delete", SetPriorityAction.NAME, new String[] { DeleteAction.NAME });
        assertInvalidAction("delete", UnfollowAction.NAME, new String[] { DeleteAction.NAME });

        Phase phase = new Phase("foo", TimeValue.ZERO, Collections.emptyMap());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.INSTANCE.getNextActionName(ShrinkAction.NAME, phase));
        assertEquals("lifecycle type[" + TimeseriesLifecycleType.TYPE + "] does not support phase[" + phase.getName() + "]",
                exception.getMessage());
    }

    private void assertNextActionName(String phaseName, String currentAction, String expectedNextAction, String... availableActionNames) {
        Map<String, LifecycleAction> availableActions = convertActionNamesToActions(availableActionNames);
        Phase phase = new Phase(phaseName, TimeValue.ZERO, availableActions);
        String nextAction = TimeseriesLifecycleType.INSTANCE.getNextActionName(currentAction, phase);
        assertEquals(expectedNextAction, nextAction);
    }

    private void assertInvalidAction(String phaseName, String currentAction, String... availableActionNames) {
        Map<String, LifecycleAction> availableActions = convertActionNamesToActions(availableActionNames);
        Phase phase = new Phase(phaseName, TimeValue.ZERO, availableActions);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.INSTANCE.getNextActionName(currentAction, phase));
        assertEquals("[" + currentAction + "] is not a valid action for phase [" + phaseName + "] in lifecycle type ["
                + TimeseriesLifecycleType.TYPE + "]", exception.getMessage());
    }

    private ConcurrentMap<String, LifecycleAction> convertActionNamesToActions(String... availableActionNames) {
        return Arrays.asList(availableActionNames).stream().map(n -> {
            switch (n) {
            case AllocateAction.NAME:
                return new AllocateAction(null, Collections.singletonMap("foo", "bar"), Collections.emptyMap(), Collections.emptyMap());
            case DeleteAction.NAME:
                return new DeleteAction();
            case ForceMergeAction.NAME:
                return new ForceMergeAction(1);
            case ReadOnlyAction.NAME:
                return new ReadOnlyAction();
            case RolloverAction.NAME:
                return new RolloverAction(ByteSizeValue.parseBytesSizeValue("0b", "test"), TimeValue.ZERO, 1L);
            case ShrinkAction.NAME:
                return new ShrinkAction(1);
            case FreezeAction.NAME:
                return new FreezeAction();
            case SetPriorityAction.NAME:
                return new SetPriorityAction(0);
            case UnfollowAction.NAME:
                return  new UnfollowAction();
            }
            return new DeleteAction();
        }).collect(Collectors.toConcurrentMap(LifecycleAction::getWriteableName, Function.identity()));
    }

    private void assertNextPhaseName(String currentPhase, String expectedNextPhase, String... availablePhaseNames) {
        Map<String, Phase> availablePhases = Arrays.asList(availablePhaseNames).stream()
                .map(n -> new Phase(n, TimeValue.ZERO, Collections.emptyMap()))
                .collect(Collectors.toMap(Phase::getName, Function.identity()));
        String nextPhase = TimeseriesLifecycleType.INSTANCE.getNextPhaseName(currentPhase, availablePhases);
        assertEquals(expectedNextPhase, nextPhase);
    }

    private void assertPreviousPhaseName(String currentPhase, String expectedNextPhase, String... availablePhaseNames) {
        Map<String, Phase> availablePhases = Arrays.asList(availablePhaseNames).stream()
                .map(n -> new Phase(n, TimeValue.ZERO, Collections.emptyMap()))
                .collect(Collectors.toMap(Phase::getName, Function.identity()));
        String nextPhase = TimeseriesLifecycleType.INSTANCE.getPreviousPhaseName(currentPhase, availablePhases);
        assertEquals(expectedNextPhase, nextPhase);
    }

    /**
     * checks whether an ordered list of objects (usually Phase and LifecycleAction) are found in the same
     * order as the ordered VALID_PHASES/VALID_HOT_ACTIONS/... lists
     * @param orderedObjects the ordered objects to verify sort order of
     * @param getKey the way to retrieve the key to sort against (Phase#getName, LifecycleAction#getName)
     * @param validOrderedKeys the source of truth of the sort order
     * @param <T> the type of object
     */
    private <T> boolean isSorted(List<T> orderedObjects, Function<T, String> getKey, List<String> validOrderedKeys) {
        int validIndex = 0;
        for (T obj : orderedObjects) {
            String key = getKey.apply(obj);
            int i = validIndex;
            for (; i < validOrderedKeys.size(); i++) {
                if (validOrderedKeys.get(i).equals(key)) {
                    validIndex = i;
                    break;
                }
            }
            if (i == validOrderedKeys.size()) {
                return false;
            }
        }
        return true;
    }

    private LifecycleAction getTestAction(String actionName) {
        switch (actionName) {
            case AllocateAction.NAME:
                return TEST_ALLOCATE_ACTION;
            case WaitForSnapshotAction.NAME:
                return TEST_WAIT_FOR_SNAPSHOT_ACTION;
            case DeleteAction.NAME:
                return TEST_DELETE_ACTION;
            case ForceMergeAction.NAME:
                return TEST_FORCE_MERGE_ACTION;
            case ReadOnlyAction.NAME:
                return TEST_READ_ONLY_ACTION;
            case RolloverAction.NAME:
                return TEST_ROLLOVER_ACTION;
            case ShrinkAction.NAME:
                return TEST_SHRINK_ACTION;
            case FreezeAction.NAME:
                return TEST_FREEZE_ACTION;
            case SetPriorityAction.NAME:
                return TEST_PRIORITY_ACTION;
            case UnfollowAction.NAME:
                return TEST_UNFOLLOW_ACTION;
            default:
                throw new IllegalArgumentException("unsupported timeseries phase action [" + actionName + "]");
        }
    }
}
