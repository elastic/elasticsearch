/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecycleType.VALID_COLD_ACTIONS;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecycleType.VALID_DELETE_ACTIONS;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecycleType.VALID_HOT_ACTIONS;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecycleType.VALID_PHASES;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecycleType.VALID_WARM_ACTIONS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TimeseriesLifecycleTypeTests extends ESTestCase {
    
    private static final AllocateAction TEST_ALLOCATE_ACTION = new AllocateAction(null, null, null);
    private static final DeleteAction TEST_DELETE_ACTION = new DeleteAction();
    private static final ForceMergeAction TEST_FORCE_MERGE_ACTION = new ForceMergeAction(1);
    private static final ReplicasAction TEST_REPLICAS_ACTION = new ReplicasAction(1);
    private static final RolloverAction TEST_ROLLOVER_ACTION = new RolloverAction("", new ByteSizeValue(1), null, null);
    private static final ShrinkAction TEST_SHRINK_ACTION = new ShrinkAction(1);

    public void testGetFirstPhase() {
        Map<String, Phase> phases = new HashMap<>();
        Phase expectedFirstPhase = null;
        for (String phaseName : Arrays.asList("hot", "warm", "cold", "delete")) {
            if (randomBoolean()) {
                Phase phase = new Phase(phaseName, TimeValue.MINUS_ONE, Collections.emptyMap());
                phases.put(phaseName, phase);
                if (expectedFirstPhase == null) {
                    expectedFirstPhase = phase;
                }
            }
        }
        TimeseriesLifecycleType policy = TimeseriesLifecycleType.INSTANCE;
        assertThat(policy.getFirstPhase(phases), equalTo(expectedFirstPhase));
    }

    public void testGetNextPhase() {
        for (int runs = 0; runs < 20; runs++) {
            Map<String, Phase> phases = new HashMap<>();
            List<Phase> phasesInOrder = new ArrayList<>();
            for (String phase : VALID_PHASES) {
                if (randomBoolean()) {
                    Phase phaseToAdd = new Phase(phase, TimeValue.MINUS_ONE, Collections.emptyMap());
                    phases.put(phase, phaseToAdd);
                    phasesInOrder.add(phaseToAdd);
                }
            }
            TimeseriesLifecycleType policy = TimeseriesLifecycleType.INSTANCE;
            assertThat(policy.nextPhase(phases, null), equalTo(policy.getFirstPhase(phases)));
            for (int i = 0; i < phasesInOrder.size() - 1; i++) {
                assertThat(policy.nextPhase(phases, phasesInOrder.get(i)), equalTo(phasesInOrder.get(i + 1)));
            }
            if (phasesInOrder.isEmpty() == false) {
                assertNull(policy.nextPhase(phases, phasesInOrder.get(phasesInOrder.size() - 1)));
            }
        }
    }

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

    public void testHotActionProvider() {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        Map<String, LifecycleAction> actions = VALID_HOT_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, "", "", 0) {

            @Override
            public boolean canExecute(Phase phase) {
                assertSame(hotPhase, phase);
                return true;
            }
        };
        TimeseriesLifecycleType policy = TimeseriesLifecycleType.INSTANCE;
        LifecyclePolicy.NextActionProvider provider = policy.getActionProvider(context, hotPhase);
        assertThat(provider.next(null), equalTo(TEST_ROLLOVER_ACTION));
        assertNull(provider.next(TEST_ROLLOVER_ACTION));
    }

    public void testWarmActionProviderWithAllActionsAndReplicasFirst() {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        Map<String, LifecycleAction> actions = VALID_WARM_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        actions.put(ReplicasAction.NAME, TEST_REPLICAS_ACTION);
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, actions);
        MockIndexLifecycleContext context =new MockIndexLifecycleContext(indexName, "", "",
            TEST_REPLICAS_ACTION.getNumberOfReplicas() + 1) {

            @Override
            public boolean canExecute(Phase phase) {
                assertSame(warmPhase, phase);
                return true;
            }
        };
        TimeseriesLifecycleType policy = TimeseriesLifecycleType.INSTANCE;
        LifecyclePolicy.NextActionProvider provider = policy.getActionProvider(context, warmPhase);
        if (actions.size() > 1) {
            int actionCount = 1;
            LifecycleAction current = provider.next(null);
            assertThat(current, equalTo(TEST_REPLICAS_ACTION));
            while (actionCount++ < actions.size()) {
                current = provider.next(current);
            }
            assertNull(provider.next(current));
            assertThat(current, equalTo(TEST_FORCE_MERGE_ACTION));
        } else {
            assertThat(provider.next(null), equalTo(TEST_REPLICAS_ACTION));
        }

    }

    public void testWarmActionProviderReplicasActionSortOrder() {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_WARM_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        actions.put(ReplicasAction.NAME, TEST_REPLICAS_ACTION);
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, actions);
        MockIndexLifecycleContext context =new MockIndexLifecycleContext(indexName, "", "",
                TEST_REPLICAS_ACTION.getNumberOfReplicas() + 1) {

            @Override
            public boolean canExecute(Phase phase) {
                assertSame(warmPhase, phase);
                return true;
            }
        };
        TimeseriesLifecycleType policy = TimeseriesLifecycleType.INSTANCE;
        LifecyclePolicy.NextActionProvider provider = policy.getActionProvider(context, warmPhase);
        assertThat(provider.next(null), equalTo(TEST_REPLICAS_ACTION));
        context = new MockIndexLifecycleContext(indexName, "", "",
                TEST_REPLICAS_ACTION.getNumberOfReplicas() - 1) {

            @Override
            public boolean canExecute(Phase phase) {
                assertSame(warmPhase, phase);
                return true;
            }
        };
        provider = policy.getActionProvider(context, warmPhase);
        if (actions.size() > 1) {
            int actionCount = 1;
            LifecycleAction current = provider.next(null);
            assertThat(current, not(equalTo(TEST_REPLICAS_ACTION)));
            while (actionCount++ < actions.size()) {
                current = provider.next(current);
            }
            assertNull(provider.next(current));
            assertThat(current, equalTo(TEST_REPLICAS_ACTION));
        } else {
            assertThat(provider.next(null), equalTo(TEST_REPLICAS_ACTION));
        }
    }

    public void testColdActionProviderAllActions() {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        Map<String, LifecycleAction> actions = VALID_COLD_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        actions.put(ReplicasAction.NAME, TEST_REPLICAS_ACTION);
        Phase coldPhase = new Phase("cold", TimeValue.ZERO, actions);
        MockIndexLifecycleContext context =new MockIndexLifecycleContext(indexName, "", "",
            TEST_REPLICAS_ACTION.getNumberOfReplicas() - 1) {

            @Override
            public boolean canExecute(Phase phase) {
                assertSame(coldPhase, phase);
                return true;
            }
        };
        TimeseriesLifecycleType policy = TimeseriesLifecycleType.INSTANCE;
        LifecyclePolicy.NextActionProvider provider = policy.getActionProvider(context, coldPhase);
        if (actions.size() > 1) {
            LifecycleAction current = provider.next(null);
            assertThat(current, equalTo(TEST_ALLOCATE_ACTION));
            assertThat(provider.next(current), equalTo(TEST_REPLICAS_ACTION));
        } else {
            assertThat(provider.next(null), equalTo(TEST_REPLICAS_ACTION));
        }

        context = new MockIndexLifecycleContext(indexName, "", "",
            TEST_REPLICAS_ACTION.getNumberOfReplicas() + 1) {

            @Override
            public boolean canExecute(Phase phase) {
                assertSame(coldPhase, phase);
                return true;
            }
        };
        provider = policy.getActionProvider(context, coldPhase);
        if (actions.size() > 1) {
            LifecycleAction current = provider.next(null);
            assertThat(current, equalTo(TEST_REPLICAS_ACTION));
            assertThat(provider.next(current), equalTo(TEST_ALLOCATE_ACTION));
        } else {
            assertThat(provider.next(null), equalTo(TEST_REPLICAS_ACTION));
        }
    }

    public void testColdActionProviderReplicasActionSortOrder() {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_COLD_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        actions.put(ReplicasAction.NAME, TEST_REPLICAS_ACTION);
        Phase coldPhase = new Phase("cold", TimeValue.ZERO, actions);
        MockIndexLifecycleContext context =new MockIndexLifecycleContext(indexName, "", "",
                TEST_REPLICAS_ACTION.getNumberOfReplicas() + 1) {

            @Override
            public boolean canExecute(Phase phase) {
                assertSame(coldPhase, phase);
                return true;
            }
        };
        TimeseriesLifecycleType policy = TimeseriesLifecycleType.INSTANCE;
        LifecyclePolicy.NextActionProvider provider = policy.getActionProvider(context, coldPhase);
        assertThat(provider.next(null), equalTo(TEST_REPLICAS_ACTION));
        context = new MockIndexLifecycleContext(indexName, "", "",
                TEST_REPLICAS_ACTION.getNumberOfReplicas() - 1) {

            @Override
            public boolean canExecute(Phase phase) {
                assertSame(coldPhase, phase);
                return true;
            }
        };
        provider = policy.getActionProvider(context, coldPhase);
        if (actions.size() > 1) {
            LifecycleAction current = provider.next(null);
            assertThat(current, equalTo(TEST_ALLOCATE_ACTION));
            assertThat(provider.next(current), equalTo(TEST_REPLICAS_ACTION));
        } else {
            assertThat(provider.next(null), equalTo(TEST_REPLICAS_ACTION));
        }
    }

    public void testDeleteActionProvider() {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        Map<String, LifecycleAction> actions = VALID_DELETE_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()));
        Phase deletePhase = new Phase("delete", TimeValue.ZERO, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, "", "", 0) {

            @Override
            public boolean canExecute(Phase phase) {
                assertSame(deletePhase, phase);
                return true;
            }
        };
        TimeseriesLifecycleType policy = TimeseriesLifecycleType.INSTANCE;
        LifecyclePolicy.NextActionProvider provider = policy.getActionProvider(context, deletePhase);
        assertThat(provider.next(null), equalTo(TEST_DELETE_ACTION));
        assertNull(provider.next(TEST_DELETE_ACTION));
    }


    private LifecycleAction getTestAction(String actionName) {
        switch (actionName) {
            case AllocateAction.NAME:
                return TEST_ALLOCATE_ACTION;
            case DeleteAction.NAME:
                return TEST_DELETE_ACTION;
            case ForceMergeAction.NAME:
                return TEST_FORCE_MERGE_ACTION;
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
