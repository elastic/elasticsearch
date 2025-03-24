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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_PHASES;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.ORDERED_VALID_WARM_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_COLD_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_DELETE_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_FROZEN_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_HOT_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.VALID_WARM_ACTIONS;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.WARM_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.validateAllSearchableSnapshotActionsUseSameRepository;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.validateFrozenPhaseHasSearchableSnapshotAction;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.validateMonotonicallyIncreasingPhaseTimings;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.validateReplicateFor;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class TimeseriesLifecycleTypeTests extends ESTestCase {

    private static final AllocateAction TEST_ALLOCATE_ACTION = new AllocateAction(2, 20, Map.of("node", "node1"), null, null);
    private static final DeleteAction TEST_DELETE_ACTION = DeleteAction.WITH_SNAPSHOT_DELETE;

    private static final WaitForSnapshotAction TEST_WAIT_FOR_SNAPSHOT_ACTION = new WaitForSnapshotAction("policy");
    private static final ForceMergeAction TEST_FORCE_MERGE_ACTION = new ForceMergeAction(1, null);
    private static final RolloverAction TEST_ROLLOVER_ACTION = new RolloverAction(
        ByteSizeValue.ofBytes(1),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    private static final ShrinkAction TEST_SHRINK_ACTION = new ShrinkAction(1, null, false);
    private static final ReadOnlyAction TEST_READ_ONLY_ACTION = new ReadOnlyAction();
    private static final SetPriorityAction TEST_PRIORITY_ACTION = new SetPriorityAction(0);

    private static final SearchableSnapshotAction TEST_SEARCHABLE_SNAPSHOT_ACTION = new SearchableSnapshotAction("repo");
    // keeping the migrate action disabled as otherwise it could conflict with the allocate action if both are randomly selected for the
    // same phase
    private static final MigrateAction TEST_MIGRATE_ACTION = MigrateAction.DISABLED;
    public static final TimeValue TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);
    private static final DownsampleAction TEST_DOWNSAMPLE_ACTION = new DownsampleAction(DateHistogramInterval.DAY, TIMEOUT);

    public void testValidatePhases() {
        boolean invalid = randomBoolean();
        String phaseName = randomFrom("hot", "warm", "cold", "delete");
        if (invalid) {
            phaseName += randomAlphaOfLength(5);
        }
        Map<String, Phase> phases = Map.of(phaseName, new Phase(phaseName, TimeValue.ZERO, Map.of()));
        if (invalid) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> TimeseriesLifecycleType.INSTANCE.validate(phases.values()));
            assertThat(e.getMessage(), equalTo("Timeseries lifecycle does not support phase [" + phaseName + "]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(phases.values());
        }
    }

    public void testValidateHotPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = VALID_HOT_ACTIONS.stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "delete", "freeze"));
            actions.put(invalidAction.getWriteableName(), invalidAction);
        }
        Map<String, Phase> hotPhase = Map.of("hot", new Phase("hot", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> TimeseriesLifecycleType.INSTANCE.validate(hotPhase.values()));
            assertThat(e.getMessage(), equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [hot]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(hotPhase.values());
        }

        {
            final Consumer<Collection<String>> validateHotActions = hotActions -> {
                final Map<String, LifecycleAction> hotActionMap = hotActions.stream()
                    .map(this::getTestAction)
                    .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
                TimeseriesLifecycleType.INSTANCE.validate(List.of(new Phase("hot", TimeValue.ZERO, hotActionMap)));
            };

            validateHotActions.accept(List.of(RolloverAction.NAME));
            validateHotActions.accept(List.of(RolloverAction.NAME, ForceMergeAction.NAME));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validateHotActions.accept(List.of(ForceMergeAction.NAME))
            );
            assertThat(
                e.getMessage(),
                containsString("the [forcemerge] action(s) may not be used in the [hot] phase without an accompanying [rollover] action")
            );
        }
    }

    public void testValidateWarmPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_WARM_ACTIONS).stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete", "freeze"));
            actions.put(invalidAction.getWriteableName(), invalidAction);
        }
        Map<String, Phase> warmPhase = Map.of("warm", new Phase("warm", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> TimeseriesLifecycleType.INSTANCE.validate(warmPhase.values()));
            assertThat(e.getMessage(), equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [warm]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(warmPhase.values());
        }
    }

    public void testValidateColdPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_COLD_ACTIONS).stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete", "forcemerge", "shrink"));
            actions.put(invalidAction.getWriteableName(), invalidAction);
        }
        Map<String, Phase> coldPhase = Map.of("cold", new Phase("cold", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> TimeseriesLifecycleType.INSTANCE.validate(coldPhase.values()));
            assertThat(e.getMessage(), equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [cold]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(coldPhase.values());
        }
    }

    public void testValidateFrozenPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_FROZEN_ACTIONS).stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        // Frozen requires the searchable snapshot action to be present
        actions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo", randomBoolean()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete", "forcemerge", "shrink"));
            actions.put(invalidAction.getWriteableName(), invalidAction);
        }
        Map<String, Phase> frozenPhase = Map.of("frozen", new Phase("frozen", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.INSTANCE.validate(frozenPhase.values())
            );
            assertThat(e.getMessage(), equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [frozen]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(frozenPhase.values());
        }
    }

    public void testValidateDeletePhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = VALID_DELETE_ACTIONS.stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "rollover", "forcemerge", "shrink", "freeze", "set_priority"));
            actions.put(invalidAction.getWriteableName(), invalidAction);
        }
        Map<String, Phase> deletePhase = Map.of("delete", new Phase("delete", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.INSTANCE.validate(deletePhase.values())
            );
            assertThat(e.getMessage(), equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [delete]"));
        } else {
            TimeseriesLifecycleType.INSTANCE.validate(deletePhase.values());
        }
    }

    public void testValidateConflictingDataMigrationConfigurations() {
        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put(TEST_MIGRATE_ACTION.getWriteableName(), MigrateAction.ENABLED);
        actions.put(TEST_ALLOCATE_ACTION.getWriteableName(), TEST_ALLOCATE_ACTION);
        List<Phase> phases = List.of(new Phase(WARM_PHASE, TimeValue.ZERO, actions), new Phase(COLD_PHASE, TimeValue.ZERO, actions));

        Exception validationException = expectThrows(
            IllegalArgumentException.class,
            () -> TimeseriesLifecycleType.INSTANCE.validate(phases)
        );
        assertThat(
            validationException.getMessage(),
            equalTo(
                "phases [warm,cold] specify an enabled migrate action and an allocate "
                    + "action with allocation rules. specify only a single data migration in each phase"
            )
        );

        // disabling the migrate action makes the phases definition valid as only the allocate action will perform data migration
        actions.put(TEST_MIGRATE_ACTION.getWriteableName(), MigrateAction.DISABLED);
        try {
            TimeseriesLifecycleType.INSTANCE.validate(phases);
        } catch (Exception e) {
            fail("not expecting a failure for phases that specify one action that migrates data" + e);
        }
    }

    public void testActionsThatCannotFollowSearchableSnapshot() {
        assertThat(ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT.size(), is(4));
        assertThat(
            ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT,
            containsInAnyOrder(ShrinkAction.NAME, FreezeAction.NAME, ForceMergeAction.NAME, DownsampleAction.NAME)
        );
    }

    public void testValidateActionsFollowingSearchableSnapshot() {
        {
            Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo")));
            Phase warmPhase = new Phase("warm", TimeValue.ZERO, Map.of(ShrinkAction.NAME, new ShrinkAction(1, null, false)));
            Phase coldPhase = new Phase("cold", TimeValue.ZERO, Map.of(FreezeAction.NAME, FreezeAction.INSTANCE));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateActionsFollowingSearchableSnapshot(List.of(hotPhase, warmPhase, coldPhase))
            );
            assertThat(
                e.getMessage(),
                is(
                    "phases [warm,cold] define one or more of [forcemerge, freeze, shrink, downsample] actions"
                        + " which are not allowed after a managed index is mounted as a searchable snapshot"
                )
            );
        }

        {
            Phase warmPhase = new Phase("warm", TimeValue.ZERO, Map.of(ShrinkAction.NAME, new ShrinkAction(1, null, false)));
            Phase coldPhase = new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo"))
            );
            Phase frozenPhase = new Phase("frozen", TimeValue.ZERO, Map.of(FreezeAction.NAME, FreezeAction.INSTANCE));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateActionsFollowingSearchableSnapshot(List.of(warmPhase, coldPhase, frozenPhase))
            );
            assertThat(
                e.getMessage(),
                is(
                    "phases [frozen] define one or more of [forcemerge, freeze, shrink, downsample] actions"
                        + " which are not allowed after a managed index is mounted as a searchable snapshot"
                )
            );
        }

        {
            Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo")));
            Phase warmPhase = new Phase("warm", TimeValue.ZERO, Map.of(ShrinkAction.NAME, new ShrinkAction(1, null, false)));
            Phase coldPhase = new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo"))
            );
            Phase frozenPhase = new Phase("frozen", TimeValue.ZERO, Map.of(FreezeAction.NAME, FreezeAction.INSTANCE));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateActionsFollowingSearchableSnapshot(
                    List.of(hotPhase, warmPhase, coldPhase, frozenPhase)
                )
            );
            assertThat(
                e.getMessage(),
                is(
                    "phases [warm,frozen] define one or more of [forcemerge, freeze, shrink, downsample] actions"
                        + " which are not allowed after a managed index is mounted as a searchable snapshot"
                )
            );
        }

        {
            Phase hot = new Phase(
                "hot",
                TimeValue.ZERO,
                Map.of(
                    RolloverAction.NAME,
                    new RolloverAction(null, null, null, 1L, null, null, null, null, null, null),
                    SearchableSnapshotAction.NAME,
                    new SearchableSnapshotAction(randomAlphaOfLengthBetween(4, 10))
                )
            );
            Phase warm = new Phase("warm", TimeValue.ZERO, Map.of(ForceMergeAction.NAME, new ForceMergeAction(1, null)));
            Phase cold = new Phase("cold", TimeValue.ZERO, Map.of(FreezeAction.NAME, FreezeAction.INSTANCE));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateActionsFollowingSearchableSnapshot(List.of(warm, hot, cold))
            );
            assertThat(
                e.getMessage(),
                is(
                    "phases [warm,cold] define one or more of [forcemerge, freeze, shrink, downsample] actions"
                        + " which are not allowed after a managed index is mounted as a searchable snapshot"
                )
            );
        }

        {
            Phase frozenPhase = new Phase(
                "frozen",
                TimeValue.ZERO,
                Map.of(FreezeAction.NAME, FreezeAction.INSTANCE, SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo"))
            );
            try {
                TimeseriesLifecycleType.validateActionsFollowingSearchableSnapshot(List.of(frozenPhase));
            } catch (Exception e) {
                fail("unexpected exception while validating phase [ " + frozenPhase + " ] but got [" + e.getMessage() + "]");
            }
        }
    }

    public void testValidateDownsamplingAction() {
        {
            Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(RolloverAction.NAME, TEST_ROLLOVER_ACTION));
            Phase warmPhase = new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of(DownsampleAction.NAME, new DownsampleAction(DateHistogramInterval.hours(1), TIMEOUT))
            );
            Phase coldPhase = new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(DownsampleAction.NAME, new DownsampleAction(DateHistogramInterval.hours(1), TIMEOUT))
            );

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateDownsamplingIntervals(List.of(warmPhase, coldPhase, hotPhase))
            );
            assertThat(
                e.getMessage(),
                is("Downsampling interval [1h] for phase [cold] must be greater than the interval [1h] for phase [warm]")
            );
        }

        {
            Phase warmPhase = new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of(DownsampleAction.NAME, new DownsampleAction(DateHistogramInterval.hours(1), TIMEOUT))
            );
            Phase coldPhase = new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(DownsampleAction.NAME, new DownsampleAction(DateHistogramInterval.minutes(30), TIMEOUT))
            );

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateDownsamplingIntervals(List.of(coldPhase, warmPhase))
            );
            assertThat(
                e.getMessage(),
                is("Downsampling interval [30m] for phase [cold] must be greater than the interval [1h] for phase [warm]")
            );
        }

        {
            Phase warmPhase = new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of(DownsampleAction.NAME, new DownsampleAction(DateHistogramInterval.hours(1), TIMEOUT))
            );
            Phase coldPhase = new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(DownsampleAction.NAME, new DownsampleAction(DateHistogramInterval.minutes(130), TIMEOUT))
            );

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> TimeseriesLifecycleType.validateDownsamplingIntervals(List.of(coldPhase, warmPhase))
            );
            assertThat(
                e.getMessage(),
                is("Downsampling interval [130m] for phase [cold] must be a multiple of the interval [1h] for phase [warm]")
            );
        }

        {
            Phase hotPhase = new Phase(
                "hot",
                TimeValue.ZERO,
                Map.of(
                    RolloverAction.NAME,
                    TEST_ROLLOVER_ACTION,
                    DownsampleAction.NAME,
                    new DownsampleAction(DateHistogramInterval.minutes(10), TIMEOUT)
                )
            );
            Phase warmPhase = new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of(DownsampleAction.NAME, new DownsampleAction(DateHistogramInterval.minutes(30), TIMEOUT))
            );
            Phase coldPhase = new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(DownsampleAction.NAME, new DownsampleAction(DateHistogramInterval.hours(2), TIMEOUT))
            );

            // This is a valid interval combination
            TimeseriesLifecycleType.validateDownsamplingIntervals(List.of(coldPhase, warmPhase, hotPhase));
        }
    }

    public void testGetOrderedPhases() {
        Map<String, Phase> phaseMap = new HashMap<>();
        for (String phaseName : randomSubsetOf(randomIntBetween(0, ORDERED_VALID_PHASES.size()), ORDERED_VALID_PHASES)) {
            phaseMap.put(phaseName, new Phase(phaseName, TimeValue.ZERO, Map.of()));
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
        assertTrue(isUnfollowInjected("cold", SearchableSnapshotAction.NAME));
        assertTrue(isUnfollowInjected("frozen", SearchableSnapshotAction.NAME));

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
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TimeseriesLifecycleType.INSTANCE.getOrderedActions(new Phase("invalid", TimeValue.ZERO, Map.of()))
        );
        assertThat(exception.getMessage(), equalTo("lifecycle type [timeseries] does not support phase [invalid]"));
    }

    public void testGetOrderedActionsHot() {
        Map<String, LifecycleAction> actions = VALID_HOT_ACTIONS.stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(hotPhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_HOT_ACTIONS));
        assertThat(orderedActions.indexOf(TEST_PRIORITY_ACTION), equalTo(0));
    }

    public void testGetOrderedActionsWarm() {
        Map<String, LifecycleAction> actions = VALID_WARM_ACTIONS.stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(warmPhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_WARM_ACTIONS));
        assertThat(orderedActions.indexOf(TEST_PRIORITY_ACTION), equalTo(0));
    }

    public void testGetOrderedActionsCold() {
        Map<String, LifecycleAction> actions = VALID_COLD_ACTIONS.stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase coldPhase = new Phase("cold", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(coldPhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_COLD_ACTIONS));
        assertThat(orderedActions.indexOf(TEST_PRIORITY_ACTION), equalTo(0));
    }

    public void testGetOrderedActionsDelete() {
        Map<String, LifecycleAction> actions = VALID_DELETE_ACTIONS.stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase deletePhase = new Phase("delete", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(deletePhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_DELETE_ACTIONS));
    }

    public void testShouldMigrateDataToTiers() {
        {
            // there's an allocate action
            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put(
                TEST_ALLOCATE_ACTION.getWriteableName(),
                randomFrom(TEST_ALLOCATE_ACTION, new AllocateAction(2, 20, null, null, null))
            );
            Phase phase = new Phase(WARM_PHASE, TimeValue.ZERO, actions);
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(true));
        }

        {
            // there's a migrate action
            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put(TEST_MIGRATE_ACTION.getWriteableName(), randomBoolean() ? MigrateAction.ENABLED : MigrateAction.DISABLED);
            Phase phase = new Phase(WARM_PHASE, TimeValue.ZERO, actions);
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(false));
        }

        {
            // there's a searchable_snapshot action
            Map<String, LifecycleAction> actions = new HashMap<>();
            actions.put(TEST_SEARCHABLE_SNAPSHOT_ACTION.getWriteableName(), TEST_SEARCHABLE_SNAPSHOT_ACTION);
            Phase phase = new Phase(randomFrom(COLD_PHASE, FROZEN_PHASE), TimeValue.ZERO, actions);
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(false));
        }

        {
            // not inject in hot phase
            Phase phase = new Phase(HOT_PHASE, TimeValue.ZERO, Map.of());
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(false));
        }

        {
            // not inject in frozen phase
            Phase phase = new Phase(FROZEN_PHASE, TimeValue.ZERO, Map.of());
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(false));
        }

        {
            // not inject in delete phase
            Phase phase = new Phase(DELETE_PHASE, TimeValue.ZERO, Map.of());
            assertThat(TimeseriesLifecycleType.shouldInjectMigrateStepForPhase(phase), is(false));
        }

        {
            // return false for invalid phase
            Phase phase = new Phase(HOT_PHASE + randomAlphaOfLength(5), TimeValue.ZERO, Map.of());
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

            validateAllSearchableSnapshotActionsUseSameRepository(List.of(hotPhase, coldPhase, frozenPhase));
        }

        {
            hotActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo1", randomBoolean()));
            coldActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo2", randomBoolean()));
            frozenActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo1", randomBoolean()));

            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.ZERO, hotActions);
            Phase coldPhase = new Phase(HOT_PHASE, TimeValue.ZERO, coldActions);
            Phase frozenPhase = new Phase(HOT_PHASE, TimeValue.ZERO, frozenActions);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validateAllSearchableSnapshotActionsUseSameRepository(List.of(hotPhase, coldPhase, frozenPhase))
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "policy specifies [searchable_snapshot]"
                        + " action multiple times with differing repositories [repo2, repo1],"
                        + " the same repository must be used for all searchable snapshot actions"
                )
            );
        }
    }

    public void testValidatingIncreasingAges() {
        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(1), Map.of());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.ZERO, Map.of());
            Phase coldPhase = new Phase(COLD_PHASE, TimeValue.ZERO, Map.of());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.ZERO, Map.of());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.ZERO, Map.of());

            assertFalse(
                Strings.hasText(
                    validateMonotonicallyIncreasingPhaseTimings(List.of(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase))
                )
            );
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(1), Map.of());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.timeValueDays(1), Map.of());
            Phase coldPhase = new Phase(COLD_PHASE, TimeValue.timeValueDays(1), Map.of());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.timeValueDays(1), Map.of());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.timeValueDays(1), Map.of());

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
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(1), Map.of());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.ZERO, Map.of());
            Phase coldPhase = new Phase(COLD_PHASE, TimeValue.timeValueHours(12), Map.of());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.ZERO, Map.of());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.ZERO, Map.of());

            String err = validateMonotonicallyIncreasingPhaseTimings(List.of(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase));

            assertThat(
                err,
                containsString(
                    "Your policy is configured to run the cold phase "
                        + "(min_age: 12h) before the hot phase (min_age: 1d). You should change "
                        + "the phase timing so that the phases will execute in the order of hot, warm, then cold."
                )
            );
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(1), Map.of());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.timeValueDays(3), Map.of());
            Phase coldPhase = new Phase(COLD_PHASE, null, Map.of());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.timeValueDays(1), Map.of());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.timeValueDays(2), Map.of());

            String err = validateMonotonicallyIncreasingPhaseTimings(List.of(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase));

            assertThat(
                err,
                containsString(
                    "Your policy is configured to run the frozen phase "
                        + "(min_age: 1d) and the delete phase (min_age: 2d) before the warm phase (min_age: 3d)."
                        + " You should change the phase timing so that the phases will execute in the order of hot, warm, then cold."
                )
            );
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(1), Map.of());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.timeValueDays(3), Map.of());
            Phase coldPhase = new Phase(COLD_PHASE, null, Map.of());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.timeValueDays(2), Map.of());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.timeValueDays(1), Map.of());

            String err = validateMonotonicallyIncreasingPhaseTimings(List.of(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase));

            assertThat(
                err,
                containsString(
                    "Your policy is configured to run the frozen phase "
                        + "(min_age: 2d) and the delete phase (min_age: 1d) before the warm phase (min_age: 3d)."
                        + " You should change the phase timing so that the phases will execute in the order of hot, warm, then cold."
                )
            );
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(3), Map.of());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.timeValueDays(2), Map.of());
            Phase coldPhase = new Phase(COLD_PHASE, null, Map.of());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.timeValueDays(2), Map.of());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.timeValueDays(1), Map.of());

            String err = validateMonotonicallyIncreasingPhaseTimings(List.of(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase));

            assertThat(
                err,
                containsString(
                    "Your policy is configured to run the frozen phase "
                        + "(min_age: 2d), the delete phase (min_age: 1d) and the warm phase (min_age: 2d) before the"
                        + " hot phase (min_age: 3d). You should change the phase timing so that the phases will execute"
                        + " in the order of hot, warm, then cold."
                )
            );
        }

        {
            Phase hotPhase = new Phase(HOT_PHASE, TimeValue.timeValueDays(3), Map.of());
            Phase warmPhase = new Phase(WARM_PHASE, TimeValue.timeValueDays(2), Map.of());
            Phase coldPhase = new Phase(COLD_PHASE, TimeValue.timeValueDays(2), Map.of());
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.timeValueDays(2), Map.of());
            Phase deletePhase = new Phase(DELETE_PHASE, TimeValue.timeValueDays(1), Map.of());

            String err = validateMonotonicallyIncreasingPhaseTimings(List.of(hotPhase, warmPhase, coldPhase, frozenPhase, deletePhase));

            assertThat(
                err,
                containsString(
                    "Your policy is configured to run the cold phase (min_age: 2d), the frozen phase "
                        + "(min_age: 2d), the delete phase (min_age: 1d) and the warm phase (min_age: 2d) before the"
                        + " hot phase (min_age: 3d). You should change the phase timing so that the phases will execute"
                        + " in the order of hot, warm, then cold."
                )
            );
        }
    }

    public void testValidateFrozenPhaseHasSearchableSnapshot() {
        {
            Map<String, LifecycleAction> frozenActions = new HashMap<>();
            frozenActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction("repo1", randomBoolean()));
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.ZERO, frozenActions);
            validateFrozenPhaseHasSearchableSnapshotAction(List.of(frozenPhase));
        }

        {
            Map<String, LifecycleAction> frozenActions = new HashMap<>();
            Phase frozenPhase = new Phase(FROZEN_PHASE, TimeValue.ZERO, frozenActions);
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validateFrozenPhaseHasSearchableSnapshotAction(List.of(frozenPhase))
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "policy specifies the [frozen] phase without a corresponding "
                        + "[searchable_snapshot] action, but a searchable snapshot action is required in the frozen phase"
                )
            );
        }
    }

    public void testValidateReplicateFor() {
        IllegalArgumentException e;

        // a searchable_snapshot action with replicate_for set to 10d
        final var searchableSnapshotAction = new SearchableSnapshotAction(
            "repo",
            randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 100), // the ESTestCase utility can produce zeroes, which we can't have here
            TimeValue.timeValueDays(10)
        );

        // first test case: there's a replicate_for, but it isn't on the first searchable_snapshot action
        e = expectThrows(
            IllegalArgumentException.class,
            () -> validateReplicateFor(
                List.of(
                    new Phase(
                        HOT_PHASE,
                        TimeValue.ZERO,
                        Map.of(RolloverAction.NAME, TEST_ROLLOVER_ACTION, SearchableSnapshotAction.NAME, searchableSnapshotAction)
                    ),
                    new Phase(COLD_PHASE, TimeValue.ZERO, Map.of(SearchableSnapshotAction.NAME, searchableSnapshotAction))
                )
            )
        );
        assertThat(
            e.getMessage(),
            is(
                "only the first searchable_snapshot action in a policy may specify 'replicate_for', "
                    + "but it was specified in the [cold] phase"
            )
        );

        // second test case: there's a replicate_for, but the next phase has a shorter min_age
        e = expectThrows(
            IllegalArgumentException.class,
            () -> validateReplicateFor(
                List.of(
                    new Phase(
                        HOT_PHASE,
                        TimeValue.ZERO,
                        Map.of(RolloverAction.NAME, TEST_ROLLOVER_ACTION, SearchableSnapshotAction.NAME, searchableSnapshotAction)
                    ),
                    new Phase(WARM_PHASE, TimeValue.timeValueDays(5), Map.of(TEST_MIGRATE_ACTION.getWriteableName(), MigrateAction.ENABLED))
                )
            )
        );
        assertThat(
            e.getMessage(),
            is(
                "The time a searchable snapshot is replicated in replicate_for [10d] may not exceed the time "
                    + "until the next phase is configured to begin. Based on the min_age [5d] of the [warm] phase, "
                    + "the maximum time the snapshot can be replicated is [5d]."
            )
        );

        // third test case: there's a replicate_for, but the implied min_age difference isn't sufficient
        e = expectThrows(
            IllegalArgumentException.class,
            () -> validateReplicateFor(
                List.of(
                    new Phase(HOT_PHASE, TimeValue.ZERO, Map.of(RolloverAction.NAME, TEST_ROLLOVER_ACTION)),
                    new Phase(COLD_PHASE, TimeValue.timeValueDays(5), Map.of(SearchableSnapshotAction.NAME, searchableSnapshotAction)),
                    new Phase(DELETE_PHASE, TimeValue.timeValueDays(12), Map.of())
                )
            )
        );
        assertThat(
            e.getMessage(),
            is(
                "The time a searchable snapshot is replicated in replicate_for [10d] may not exceed the time "
                    + "until the next phase is configured to begin. Based on the min_age [12d] of the [delete] phase, "
                    + "the maximum time the snapshot can be replicated is [7d]."
            )
        );
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
        return switch (actionName) {
            case AllocateAction.NAME -> TEST_ALLOCATE_ACTION;
            case WaitForSnapshotAction.NAME -> TEST_WAIT_FOR_SNAPSHOT_ACTION;
            case DeleteAction.NAME -> TEST_DELETE_ACTION;
            case ForceMergeAction.NAME -> TEST_FORCE_MERGE_ACTION;
            case ReadOnlyAction.NAME -> TEST_READ_ONLY_ACTION;
            case RolloverAction.NAME -> TEST_ROLLOVER_ACTION;
            case ShrinkAction.NAME -> TEST_SHRINK_ACTION;
            case FreezeAction.NAME -> FreezeAction.INSTANCE;
            case SetPriorityAction.NAME -> TEST_PRIORITY_ACTION;
            case UnfollowAction.NAME -> UnfollowAction.INSTANCE;
            case SearchableSnapshotAction.NAME -> TEST_SEARCHABLE_SNAPSHOT_ACTION;
            case MigrateAction.NAME -> TEST_MIGRATE_ACTION;
            case DownsampleAction.NAME -> TEST_DOWNSAMPLE_ACTION;
            default -> throw new IllegalArgumentException("unsupported timeseries phase action [" + actionName + "]");
        };
    }
}
