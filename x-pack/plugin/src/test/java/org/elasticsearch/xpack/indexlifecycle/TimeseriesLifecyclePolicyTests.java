/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_COLD_ACTIONS;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_DELETE_ACTIONS;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_HOT_ACTIONS;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_PHASES;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_WARM_ACTIONS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TimeseriesLifecyclePolicyTests extends AbstractSerializingTestCase<LifecyclePolicy> {
    
    private NamedXContentRegistry registry;
    private String lifecycleName;
    private static final AllocateAction TEST_ALLOCATE_ACTION = new AllocateAction();
    private static final DeleteAction TEST_DELETE_ACTION = new DeleteAction();
    private static final ForceMergeAction TEST_FORCE_MERGE_ACTION = new ForceMergeAction();
    private static final ReplicasAction TEST_REPLICAS_ACTION = new ReplicasAction(1);
    private static final RolloverAction TEST_ROLLOVER_ACTION = new RolloverAction("", new ByteSizeValue(1), null, null);
    private static final ShrinkAction TEST_SHRINK_ACTION = new ShrinkAction();
    private static final List<String> VALID_PHASE_NAMES = Arrays.asList();

    @Before
    public void setup() {
        List<NamedXContentRegistry.Entry> entries = Arrays
            .asList(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
                new NamedXContentRegistry.Entry(LifecyclePolicy.class, new ParseField(TimeseriesLifecyclePolicy.TYPE),
                    TimeseriesLifecyclePolicy::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReplicasAction.NAME), ReplicasAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse));
        registry = new NamedXContentRegistry(entries);
        lifecycleName = randomAlphaOfLength(20); // NOCOMMIT we need to randomise the lifecycle name rather 
                                                 // than use the same name for all instances
    }

    @Override
    protected LifecyclePolicy createTestInstance() {
        Map<String, Phase> phases = TimeseriesLifecyclePolicy.VALID_PHASES.stream()
                .map(phaseName -> new Phase(phaseName,
                        TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after"), Collections.emptyMap()))
                .collect(Collectors.toMap(Phase::getName, Function.identity()));
        return new TimeseriesLifecyclePolicy(lifecycleName, phases);
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
            phases = randomValueOtherThan(phases,
                    () -> TimeseriesLifecyclePolicy.VALID_PHASES.stream()
                            .map(phaseName -> new Phase(phaseName,
                                    TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after"),
                                    Collections.emptyMap()))
                            .collect(Collectors.toMap(Phase::getName, Function.identity())));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new TimeseriesLifecyclePolicy(name, phases);
    }

    @Override
    protected LifecyclePolicy doParseInstance(XContentParser parser) throws IOException {
        return LifecyclePolicy.parse(parser, new Tuple<>(lifecycleName, registry));
    }

    @Override
    protected Reader<LifecyclePolicy> instanceReader() {
        return TimeseriesLifecyclePolicy::new;
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new),
                    new NamedWriteableRegistry.Entry(LifecycleAction.class, AllocateAction.NAME, AllocateAction::new),
                    new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new),
                    new NamedWriteableRegistry.Entry(LifecycleAction.class, ReplicasAction.NAME, ReplicasAction::new),
                    new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::new),
                    new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new)));
    }

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
        TimeseriesLifecyclePolicy policy = new TimeseriesLifecyclePolicy(lifecycleName, phases);
        assertThat(policy.getFirstPhase(), equalTo(expectedFirstPhase));
    }

    public void testGetNextPhase() {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            Map<String, Phase> phases = new HashMap<>();
            List<Phase> phasesInOrder = new ArrayList<>();
            for (String phase : VALID_PHASES) {
                if (randomBoolean()) {
                    Phase phaseToAdd = new Phase(phase, TimeValue.MINUS_ONE, Collections.emptyMap());
                    phases.put(phase, phaseToAdd);
                    phasesInOrder.add(phaseToAdd);
                }
            }
            TimeseriesLifecyclePolicy policy = new TimeseriesLifecyclePolicy(lifecycleName, phases);
            assertThat(policy.nextPhase(null), equalTo(policy.getFirstPhase()));
            for (int i = 0; i < phasesInOrder.size() - 1; i++) {
                assertThat(policy.nextPhase(phasesInOrder.get(i)), equalTo(phasesInOrder.get(i + 1)));
            }
            if (phasesInOrder.isEmpty() == false) {
                assertNull(policy.nextPhase(phasesInOrder.get(phasesInOrder.size() - 1)));
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
            Exception e = expectThrows(IllegalArgumentException.class, () -> new TimeseriesLifecyclePolicy(lifecycleName, phases));
            assertThat(e.getMessage(), equalTo("Timeseries lifecycle does not support phase [" + phaseName + "]"));
        } else {
            new TimeseriesLifecyclePolicy(lifecycleName, phases);
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
                () -> new TimeseriesLifecyclePolicy(lifecycleName, hotPhase));
            assertThat(e.getMessage(),
                equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [hot]"));
        } else {
            new TimeseriesLifecyclePolicy(lifecycleName, hotPhase);
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
                () -> new TimeseriesLifecyclePolicy(lifecycleName, warmPhase));
            assertThat(e.getMessage(),
            equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [warm]"));
        } else {
            new TimeseriesLifecyclePolicy(lifecycleName, warmPhase);
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
                () -> new TimeseriesLifecyclePolicy(lifecycleName, coldPhase));
            assertThat(e.getMessage(),
                equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [cold]"));
        } else {
            new TimeseriesLifecyclePolicy(lifecycleName, coldPhase);
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
                () -> new TimeseriesLifecyclePolicy(lifecycleName, deletePhase));
            assertThat(e.getMessage(),
                equalTo("invalid action [" + invalidAction.getWriteableName() + "] defined in phase [delete]"));
        } else {
            new TimeseriesLifecyclePolicy(lifecycleName, deletePhase);
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
        TimeseriesLifecyclePolicy policy = new TimeseriesLifecyclePolicy(lifecycleName, Collections.singletonMap("hot", hotPhase));
        LifecyclePolicy.NextActionProvider provider = policy.getActionProvider(context, hotPhase);
        assertThat(provider.next(null), equalTo(TEST_ROLLOVER_ACTION));
        assertNull(provider.next(TEST_ROLLOVER_ACTION));
    }

    @AwaitsFix(bugUrl = "This gets into an infinite loop if there are other actions as well as the replicas action")
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
        TimeseriesLifecyclePolicy policy = new TimeseriesLifecyclePolicy(lifecycleName, Collections.singletonMap("warm", warmPhase));
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
            LifecycleAction current = provider.next(null);
            assertThat(current, not(equalTo(TEST_REPLICAS_ACTION)));
            while (true) {
                // NOCOMMIT This loop never exits as there is no break condition
                // also provider.next(current) never evaluates to null because
                // when called with the replicas action it always returns a
                // non-null action. We should avoid using while true here
                // because it means if there is a bug we will hang the build
                if (provider.next(current) == null) {
                    assertThat(current, equalTo(TEST_REPLICAS_ACTION));
                } else {
                    current = provider.next(current);
                }
            }
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
        TimeseriesLifecyclePolicy policy = new TimeseriesLifecyclePolicy(lifecycleName, Collections.singletonMap("cold", coldPhase));
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
        TimeseriesLifecyclePolicy policy = new TimeseriesLifecyclePolicy(lifecycleName, Collections.singletonMap("delete", deletePhase));
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
