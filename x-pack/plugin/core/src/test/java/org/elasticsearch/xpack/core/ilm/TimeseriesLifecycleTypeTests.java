/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.COLD_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.DELETE_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.FROZEN_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.HOT_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_COLD_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_DELETE_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_HOT_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_WARM_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_COLD_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_DELETE_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_HOT_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_PHASES;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_WARM_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.WARM_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.validateAllSearchableSnapshotActionsUseSameRepository;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.validateMonotonicallyIncreasingPhaseTimings;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.validateFrozenPhaseHasSearchableSnapshotAction;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class TimeseriesLifecycleTypeTests extends ESTestCase {

    private static final AllocateAction TEST_ALLOCATE_ACTION =
        new AllocateAction(2, 20, Collections.singletonMap("node", "node1"),null, null);
    private static final DeleteAction TEST_DELETE_ACTION = new DeleteAction();
    private static final WaitForSnapshotAction TEST_WAIT_FOR_SNAPSHOT_ACTION = new WaitForSnapshotAction("policy");
    private static final ForceMergeAction TEST_FORCE_MERGE_ACTION = new ForceMergeAction(1, null);
    private static final RolloverAction TEST_ROLLOVER_ACTION = new RolloverAction(new ByteSizeValue(1), null, null, null);
    private static final ShrinkAction TEST_SHRINK_ACTION = new ShrinkAction(1, null);
    private static final ReadOnlyAction TEST_READ_ONLY_ACTION = new ReadOnlyAction();
    private static final FreezeAction TEST_FREEZE_ACTION = new FreezeAction();
    private static final SetPriorityAction TEST_PRIORITY_ACTION = new SetPriorityAction(0);
    private static final UnfollowAction TEST_UNFOLLOW_ACTION  = new UnfollowAction();
    private static final SearchableSnapshotAction TEST_SEARCHABLE_SNAPSHOT_ACTION = new SearchableSnapshotAction("repo");
    // keeping the migrate action disabled as otherwise it could conflict with the allocate action if both are randomly selected for the
    // same phase
    private static final MigrateAction TEST_MIGRATE_ACTION = new MigrateAction(false);
    private static final RollupILMAction TEST_ROLLUP_ACTION =new RollupILMAction(new RollupActionConfig(
        new RollupActionGroupConfig(new RollupActionDateHistogramGroupConfig.FixedInterval("field", DateHistogramInterval.DAY)),
        Collections.singletonList(new MetricConfig("field", Collections.singletonList("max")))), null);

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
            invalidAction = getTestAction(randomFrom("allocate", "delete", "freeze"));
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

        {
            final Consumer<Collection<String>> validateHotActions = hotActions -> {
                final Map<String, LifecycleAction> hotActionMap = hotActions.stream()
                    .map(this::getTestAction)
                    .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
                TimeseriesLifecycleType.INSTANCE.validate(Collections.singleton(new Phase("hot", TimeValue.ZERO, hotActionMap)));
            };

            validateHotActions.accept(Arrays.asList(RolloverAction.NAME));
            validateHotActions.accept(Arrays.asList(RolloverAction.NAME, ForceMergeAction.NAME));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> validateHotActions.accept(Arrays.asList(ForceMergeAction.NAME)));
            assertThat(e.getMessage(),
                containsString("the [forcemerge] action(s) may not be used in the [hot] phase without an accompanying [rollover] action"));
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

    public void testValidateConflictingDataMigrationConfigurations() {
        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put(TEST_MIGRATE_ACTION.getWriteableName(), new MigrateAction(true));
        actions.put(TEST_ALLOCATE_ACTION.getWriteableName(), TEST_ALLOCATE_ACTION);
        List<Phase> phases = List.of(new Phase(WARM_PHASE, TimeValue.ZERO, actions), new Phase(COLD_PHASE, TimeValue.ZERO, actions));

        Exception validationException = expectThrows(IllegalArgumentException.class,
            () -> TimeseriesLifecycleType.INSTANCE.validate(phases));
        assertThat(validationException.getMessage(), equalTo("phases [warm,cold] specify an enabled migrate action and an allocate " +
            "action with allocation rules. specify only a single data migration in each phase"));

        // disabling the migrate action makes the phases definition valid as only the allocate action will perform data migration
        actions.put(TEST_MIGRATE_ACTION.getWriteableName(), new MigrateAction(false));
        try {
            TimeseriesLifecycleType.INSTANCE.validate(phases);
        } catch (Exception e) {
            fail("not expecting a failure for phases that specify one action that migrates data" + e);
        }
    }

    public void testActionsThatCannotFollowSearchableSnapshot() {
        assertThat(ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT.size(), is(4));
        assertThat(ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT, containsInAnyOrder(ShrinkAction.NAME, FreezeAction.NAME,
            ForceMergeAction.NAME, RollupILMAction.NAME));
    }

    public void testValidateActionsFollowingSearchableSnapshot() {
        {
            Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo")));
            Phase warmPhase = new Phase("warm", TimeValue.ZERO, Map.of(ShrinkAction.NAME, new ShrinkAction(1, null)));
            Phase coldPhase = new Phase("cold", TimeValue.ZERO, Map.of(FreezeAction.NAME, new FreezeAction()));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateActionsFollowingSearchableSnapshot(List.of(hotPhase, warmPhase, coldPhase)));
            assertThat(e.getMessage(), is(
                "phases [warm,cold] define one or more of [forcemerge, freeze, shrink, rollup] actions" +
                    " which are not allowed after a managed index is mounted as a searchable snapshot"));
        }

        {
            Phase warmPhase = new Phase("warm", TimeValue.ZERO,
                Map.of(ShrinkAction.NAME, new ShrinkAction(1, null)));
            Phase coldPhase = new Phase("cold", TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo")));
            Phase frozenPhase = new Phase("frozen", TimeValue.ZERO,
                Map.of(FreezeAction.NAME, new FreezeAction()));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateActionsFollowingSearchableSnapshot(List.of(warmPhase, coldPhase, frozenPhase)));
            assertThat(e.getMessage(), is(
                "phases [frozen] define one or more of [forcemerge, freeze, shrink, rollup] actions" +
                    " which are not allowed after a managed index is mounted as a searchable snapshot"));
        }

        {
            Phase hotPhase = new Phase("hot", TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo")));
            Phase warmPhase = new Phase("warm", TimeValue.ZERO,
                Map.of(ShrinkAction.NAME, new ShrinkAction(1, null)));
            Phase coldPhase = new Phase("cold", TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo")));
            Phase frozenPhase = new Phase("frozen", TimeValue.ZERO,
                Map.of(FreezeAction.NAME, new FreezeAction()));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateActionsFollowingSearchableSnapshot(List.of(hotPhase, warmPhase, coldPhase,
                    frozenPhase)));
            assertThat(e.getMessage(), is(
                "phases [warm,frozen] define one or more of [forcemerge, freeze, shrink, rollup] actions" +
                    " which are not allowed after a managed index is mounted as a searchable snapshot"));
        }

        {
            Phase hot = new Phase("hot", TimeValue.ZERO, Map.of(RolloverAction.NAME, new RolloverAction(null, null, null, 1L),
            SearchableSnapshotAction.NAME, new SearchableSnapshotAction(randomAlphaOfLengthBetween(4, 10))));
            Phase warm = new Phase("warm", TimeValue.ZERO, Map.of(ForceMergeAction.NAME, new ForceMergeAction(1, null)));
            Phase cold = new Phase("cold", TimeValue.ZERO, Map.of(FreezeAction.NAME, new FreezeAction()));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateActionsFollowingSearchableSnapshot(List.of(warm, hot, cold)));
            assertThat(e.getMessage(), is(
                "phases [warm,cold] define one or more of [forcemerge, freeze, shrink, rollup] actions" +
                    " which are not allowed after a managed index is mounted as a searchable snapshot"));
        }

        {
            Phase frozenPhase = new Phase("frozen", TimeValue.ZERO, Map.of(FreezeAction.NAME, new FreezeAction(),
                SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo")));
            try {
                TimeseriesLifecycleType.validateActionsFollowingSearchableSnapshot(List.of(frozenPhase));
            } catch (Exception e) {
                fail("unexpected exception while validating phase [ "+ frozenPhase +" ] but got [" + e.getMessage()+ "]");
            }
        }
    }

    public void testGetOrderedPhases() {
        Map<String, Phase> phaseMap = new HashMap<>();
        for (String phaseName : randomSubsetOf(randomIntBetween(0, ORDERED_VALID_PHASES.size()), ORDERED_VALID_PHASES)) {
            phaseMap.put(phaseName, new Phase(phaseName, TimeValue.ZERO, Collections.emptyMap()));
        }


        assertTrue(isSorted(TimeseriesLifecycleType.INSTANCE.getOrderedPhases(phaseMap), Phase::getName, ORDERED_VALID_PHASES));
    }

    public void testGetOrderedPhasesInsertsMigrateAction() {
        Map<String, Phase> phaseMap = new HashMap<>();
        phaseMap.put(HOT_PHASE, new Phase(HOT_PHASE, TimeValue.ZERO, Map.of()));
        phaseMap.put(WARM_PHASE, new Phase(WARM_PHASE, TimeValue.ZERO, Map.of()));

        List<Phase> orderedPhases = TimeseriesLifecycleType.INSTANCE.getOrderedPhases(phaseMap);
        assertTrue(isSorted(orderedPhases, Phase::getName, ORDERED_VALID_PHASES));
        Phase warmPhase = orderedPhases.get(1);
        assertThat(warmPhase, is(notNullValue()));
        assertThat(warmPhase.getActions().get(MigrateAction.NAME), is(notNullValue()));
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
        assertThat(exception.getMessage(), equalTo("lifecycle type [timeseries] does not support phase [invalid]"));
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
        assertNextPhaseName("hot", "warm", new String[] { "hot", "warm", "cold", "delete"});
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
        assertNextActionName("hot", RolloverAction.NAME, ForceMergeAction.NAME, ForceMergeAction.NAME, RolloverAction.NAME);
        assertNextActionName("hot", ForceMergeAction.NAME, null, RolloverAction.NAME, ForceMergeAction.NAME);

        assertInvalidAction("hot", "foo", new String[] { RolloverAction.NAME });
        assertInvalidAction("hot", AllocateAction.NAME, new String[] { RolloverAction.NAME });
        assertInvalidAction("hot", DeleteAction.NAME, new String[] { RolloverAction.NAME });

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

        assertNextActionName("cold", UnfollowAction.NAME, ReadOnlyAction.NAME,
            new String[] {ReadOnlyAction.NAME, SearchableSnapshotAction.NAME, SetPriorityAction.NAME, AllocateAction.NAME});
        assertNextActionName("cold", UnfollowAction.NAME, SearchableSnapshotAction.NAME,
            new String[] {SearchableSnapshotAction.NAME, AllocateAction.NAME, FreezeAction.NAME});
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
        assertEquals("lifecycle type [" + TimeseriesLifecycleType.TYPE + "] does not support phase [" + phase.getName() + "]",
                exception.getMessage());
    }

    public void testShouldMigrateDataToTiers() {
        {
            // the allocate action contain allocation rules
            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put(TEST_MIGRATE_ACTION.getWriteableName(), new MigrateAction(false));
            actions.put(TEST_ALLOCATE_ACTION.getWriteableName(), TEST_ALLOCATE_ACTION);
            Phase phase = new Phase(WARM_PHASE, TimeValue.ZERO, actions);
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(false));
        }

        {
            // the allocate action only specifies the number of replicas
            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put(TEST_ALLOCATE_ACTION.getWriteableName(), new AllocateAction(2, 20, null, null, null));
            Phase phase = new Phase(WARM_PHASE, TimeValue.ZERO, actions);
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(true));
        }

        {
            // there's an enabled migrate action specified
            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put(TEST_MIGRATE_ACTION.getWriteableName(), new MigrateAction(true));
            Phase phase = new Phase(WARM_PHASE, TimeValue.ZERO, actions);
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(false));
        }

        {
            // there's a disabled migrate action specified
            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put(TEST_MIGRATE_ACTION.getWriteableName(), new MigrateAction(false));
            Phase phase = new Phase(WARM_PHASE, TimeValue.ZERO, actions);
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(false));
        }

        {
            // test phase defines a `searchable_snapshot` action
            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put(TEST_SEARCHABLE_SNAPSHOT_ACTION.getWriteableName(), TEST_SEARCHABLE_SNAPSHOT_ACTION);
            Phase phase = new Phase(COLD_PHASE, TimeValue.ZERO, actions);
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(false));
        }

        {
            // test `frozen` phase defines a `searchable_snapshot` action
            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put(TEST_SEARCHABLE_SNAPSHOT_ACTION.getWriteableName(), TEST_SEARCHABLE_SNAPSHOT_ACTION);
            Phase phase = new Phase(FROZEN_PHASE, TimeValue.ZERO, actions);
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(false));
        }

    }

    public void testValidatingSearchableSnapshotRepos() {
        Map<String, LifecycleAction> hotActions = new HashMap<>();
        Map<String, LifecycleAction> coldActions = new HashMap<>();
        Map<String, LifecycleAction> frozenActions = new HashMap<>();

        {
            hotActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo1", randomBoolean()));
            coldActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo1", randomBoolean()));
            frozenActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo1", randomBoolean()));

            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.ZERO, hotActions);
            Phase coldPhase = new Phase(HOT_PHASE, TimeValue.ZERO, coldActions);
            Phase frozenPhase = new Phase(HOT_PHASE, TimeValue.ZERO, frozenActions);

            validateAllSearchableSnapshotActionsUseSameRepository(Arrays.asList(hotPhase, coldPhase, frozenPhase));
        }

        {
            hotActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo1", randomBoolean()));
            coldActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo2", randomBoolean()));
            frozenActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo1", randomBoolean()));

            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.ZERO, hotActions);
            Phase coldPhase = new Phase(HOT_PHASE, TimeValue.ZERO, coldActions);
            Phase frozenPhase = new Phase(HOT_PHASE, TimeValue.ZERO, frozenActions);

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> validateAllSearchableSnapshotActionsUseSameRepository(Arrays.asList(hotPhase, coldPhase, frozenPhase)));
            assertThat(e.getMessage(), containsString("policy specifies [searchable_snapshot]" +
                " action multiple times with differing repositories [repo2, repo1]," +
                " the same repository must be used for all searchable snapshot actions"));
        }
    }

    public void testValidatingIncreasingAges() {
        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.ZERO, Collections.emptyMap());
            Phase coldPhase = new Phase(COLD_PHASE, TimeValue.ZERO, Collections.emptyMap());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.ZERO, Collections.emptyMap());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.ZERO, Collections.emptyMap());

            assertFalse(Strings.hasText(validateMonotonicallyIncreasingPhaseTimings(Arrays.asList(hotPhase,
                warmPhase, coldPhase, frozenPhase, deletePhase))));
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());
            Phase coldPhase = new Phase(COLD_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());

            List<Phase> phases = new ArrayList<>();
            phases.add(hotPhase);
            if (randomBoolean()) {
                phases.add(warmPhase);
            }
            if (randomBoolean()) {
                phases.add(coldPhase);
            }
            if (randomBoolean()) {
                phases.add(frozenPhase);
            }
            if (randomBoolean()) {
                phases.add(deletePhase);
            }
            assertFalse(Strings.hasText(validateMonotonicallyIncreasingPhaseTimings(phases)));
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.ZERO, Collections.emptyMap());
            Phase coldPhase = new Phase(COLD_PHASE, TimeValue.timeValueHours(12), Collections.emptyMap());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.ZERO, Collections.emptyMap());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.ZERO, Collections.emptyMap());

            String err =
                validateMonotonicallyIncreasingPhaseTimings(Arrays.asList(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase));

            assertThat(err,
                containsString("Your policy is configured to run the cold phase "+
                    "(min_age: 12h) before the hot phase (min_age: 1d). You should change "+
                    "the phase timing so that the phases will execute in the order of hot, warm, then cold."));
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.timeValueDays(3), Collections.emptyMap());
            Phase coldPhase = new Phase(COLD_PHASE, null, Collections.emptyMap());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.timeValueDays(2), Collections.emptyMap());

            String err =
                validateMonotonicallyIncreasingPhaseTimings(Arrays.asList(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase));

            assertThat(err,
                containsString("Your policy is configured to run the frozen phase "+
                    "(min_age: 1d) and the delete phase (min_age: 2d) before the warm phase (min_age: 3d)."+
                    " You should change the phase timing so that the phases will execute in the order of hot, warm, then cold."));
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.timeValueDays(3), Collections.emptyMap());
            Phase coldPhase = new Phase(COLD_PHASE, null, Collections.emptyMap());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.timeValueDays(2), Collections.emptyMap());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());

            String err =
                validateMonotonicallyIncreasingPhaseTimings(Arrays.asList(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase));

            assertThat(err,
                containsString("Your policy is configured to run the frozen phase "+
                    "(min_age: 2d) and the delete phase (min_age: 1d) before the warm phase (min_age: 3d)."+
                    " You should change the phase timing so that the phases will execute in the order of hot, warm, then cold."));
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(3), Collections.emptyMap());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.timeValueDays(2), Collections.emptyMap());
            Phase coldPhase = new Phase(COLD_PHASE, null, Collections.emptyMap());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.timeValueDays(2), Collections.emptyMap());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());

            String err =
                validateMonotonicallyIncreasingPhaseTimings(Arrays.asList(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase));

            assertThat(err,
                containsString("Your policy is configured to run the frozen phase "+
                    "(min_age: 2d), the delete phase (min_age: 1d) and the warm phase (min_age: 2d) before the hot phase (min_age: 3d)."+
                    " You should change the phase timing so that the phases will execute in the order of hot, warm, then cold."));
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(3), Collections.emptyMap());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.timeValueDays(2), Collections.emptyMap());
            Phase coldPhase = new Phase(COLD_PHASE, TimeValue.timeValueDays(2), Collections.emptyMap());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.timeValueDays(2), Collections.emptyMap());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.timeValueDays(1), Collections.emptyMap());

            String err =
                validateMonotonicallyIncreasingPhaseTimings(Arrays.asList(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase));

            assertThat(err,
                containsString("Your policy is configured to run the cold phase (min_age: 2d), the frozen phase "+
                    "(min_age: 2d), the delete phase (min_age: 1d) and the warm phase (min_age: 2d) before the hot phase (min_age: 3d)."+
                    " You should change the phase timing so that the phases will execute in the order of hot, warm, then cold."));
        }
    }

    public void testValidateFrozenPhaseHasSearchableSnapshot() {
        {
            Map<String, LifecycleAction> frozenActions = new HashMap<>();
            frozenActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo1", randomBoolean()));
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.ZERO, frozenActions);
            validateFrozenPhaseHasSearchableSnapshotAction(Collections.singleton(frozenPhase));
        }

        {
            Map<String, LifecycleAction> frozenActions = new HashMap<>();
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.ZERO, frozenActions);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> validateFrozenPhaseHasSearchableSnapshotAction(Collections.singleton(frozenPhase)));
            assertThat(e.getMessage(), containsString("policy specifies the [frozen] phase without a corresponding " +
                "[searchable_snapshot] action, but a searchable snapshot action is required in the frozen phase"));
        }
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
                return new AllocateAction(null, null, Collections.singletonMap("foo", "bar"), Collections.emptyMap(),
                    Collections.emptyMap());
            case DeleteAction.NAME:
                return new DeleteAction();
            case ForceMergeAction.NAME:
                return new ForceMergeAction(1, null);
            case ReadOnlyAction.NAME:
                return new ReadOnlyAction();
            case RolloverAction.NAME:
                return new RolloverAction(
                    ByteSizeValue.parseBytesSizeValue("0b", "test"),
                    ByteSizeValue.parseBytesSizeValue("0b", "test"),
                    TimeValue.ZERO,
                    1L);
            case ShrinkAction.NAME:
                return new ShrinkAction(1, null);
            case FreezeAction.NAME:
                return new FreezeAction();
            case SetPriorityAction.NAME:
                return new SetPriorityAction(0);
            case UnfollowAction.NAME:
                return new UnfollowAction();
            case MigrateAction.NAME:
                return new MigrateAction(true);
            case RollupILMAction.NAME:
                return TEST_ROLLUP_ACTION;
            case SearchableSnapshotAction.NAME:
                return TEST_SEARCHABLE_SNAPSHOT_ACTION;
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
            case SearchableSnapshotAction.NAME:
                return TEST_SEARCHABLE_SNAPSHOT_ACTION;
            case MigrateAction.NAME:
                return TEST_MIGRATE_ACTION;
            case RollupILMAction.NAME:
                return TEST_ROLLUP_ACTION;
            default:
                throw new IllegalArgumentException("unsupported timeseries phase action [" + actionName + "]");
        }
    }
}
