/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType.ORDERED_VALID_COLD_ACTIONS;
import static org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType.ORDERED_VALID_DELETE_ACTIONS;
import static org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType.ORDERED_VALID_HOT_ACTIONS;
import static org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType.ORDERED_VALID_WARM_ACTIONS;
import static org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType.VALID_COLD_ACTIONS;
import static org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType.VALID_DELETE_ACTIONS;
import static org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType.VALID_HOT_ACTIONS;
import static org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType.VALID_PHASES;
import static org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType.VALID_WARM_ACTIONS;
import static org.hamcrest.Matchers.equalTo;

public class TimeseriesLifecycleTypeTests extends ESTestCase {

    private static final AllocateAction TEST_ALLOCATE_ACTION = new AllocateAction(Collections.singletonMap("node", "node1"),null, null);
    private static final DeleteAction TEST_DELETE_ACTION = new DeleteAction();
    private static final ForceMergeAction TEST_FORCE_MERGE_ACTION = new ForceMergeAction(1);
    private static final ReplicasAction TEST_REPLICAS_ACTION = new ReplicasAction(1);
    private static final RolloverAction TEST_ROLLOVER_ACTION = new RolloverAction(new ByteSizeValue(1), null, null);
    private static final ShrinkAction TEST_SHRINK_ACTION = new ShrinkAction(1);
    private static final ReadOnlyAction TEST_READ_ONLY_ACTION = new ReadOnlyAction();

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
            invalidAction = getTestAction(randomFrom("allocate", "forcemerge", "delete", "replicas", "shrink"));
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
            invalidAction = getTestAction(randomFrom("rollover", "delete"));
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
            invalidAction = getTestAction(randomFrom("allocate", "rollover", "replicas", "forcemerge", "shrink"));
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
    }

    public void testGetOrderedActionsWarm() {
        Map<String, LifecycleAction> actions = VALID_WARM_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(warmPhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_WARM_ACTIONS));
    }

    public void testGetOrderedActionsCold() {
        Map<String, LifecycleAction> actions = VALID_COLD_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase coldPhase = new Phase("cold", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(coldPhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_COLD_ACTIONS));
    }

    public void testGetOrderedActionsDelete() {
        Map<String, LifecycleAction> actions = VALID_DELETE_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase deletePhase = new Phase("delete", TimeValue.ZERO, actions);
        List<LifecycleAction> orderedActions = TimeseriesLifecycleType.INSTANCE.getOrderedActions(deletePhase);
        assertTrue(isSorted(orderedActions, LifecycleAction::getWriteableName, ORDERED_VALID_DELETE_ACTIONS));
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
            case DeleteAction.NAME:
                return TEST_DELETE_ACTION;
            case ForceMergeAction.NAME:
                return TEST_FORCE_MERGE_ACTION;
            case ReadOnlyAction.NAME:
                return TEST_READ_ONLY_ACTION;
            case ReplicasAction.NAME:
                return TEST_REPLICAS_ACTION;
            case RolloverAction.NAME:
                return TEST_ROLLOVER_ACTION;
            case ShrinkAction.NAME:
                return TEST_SHRINK_ACTION;
            default:
                throw new IllegalArgumentException("unsupported timeseries phase action [" + actionName + "]");
        }
    }
}
