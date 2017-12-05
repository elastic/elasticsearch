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
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_COLD_ACTIONS;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_DELETE_ACTIONS;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_HOT_ACTIONS;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_PHASES;
import static org.elasticsearch.xpack.indexlifecycle.TimeseriesLifecyclePolicy.VALID_WARM_ACTIONS;
import static org.hamcrest.Matchers.equalTo;

public class TimeseriesLifecyclePolicyTests extends AbstractSerializingTestCase<LifecyclePolicy> {
    
    private NamedXContentRegistry registry;
    private String lifecycleName;
    private static final AllocateAction TEST_ALLOCATE_ACTION = new AllocateAction();
    private static final DeleteAction TEST_DELETE_ACTION = new DeleteAction();
    private static final ForceMergeAction TEST_FORCE_MERGE_ACTION = new ForceMergeAction();
    private static final ReplicasAction TEST_REPLICAS_ACTION = new ReplicasAction();
    private static final RolloverAction TEST_ROLLOVER_ACTION = new RolloverAction();
    private static final ShrinkAction TEST_SHRINK_ACTION = new ShrinkAction();

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
        return new TimeseriesLifecyclePolicy(lifecycleName, Collections.emptyMap());
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
                Phase phase = new Phase(phaseName, TimeValue.MINUS_ONE, Collections.emptyList());
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
                    Phase phaseToAdd = new Phase(phase, TimeValue.MINUS_ONE, Collections.emptyList());
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
            new Phase(phaseName, TimeValue.ZERO, Collections.emptyList()));
        if (invalid) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> new TimeseriesLifecyclePolicy(lifecycleName, phases));
            assertThat(e.getMessage(), equalTo("Timeseries lifecycle does not support phase [" + phaseName + "]"));
        } else {
            new TimeseriesLifecyclePolicy(lifecycleName, phases);
        }
    }

    public void testValidateHotPhase() {
        LifecycleAction invalidAction = null;
        List<LifecycleAction> actions = randomSubsetOf(VALID_HOT_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toList());
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "forcemerge", "delete", "replicas", "shrink"));
            actions.add(invalidAction);
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
        List<LifecycleAction> actions = randomSubsetOf(VALID_WARM_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toList());
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete"));
            actions.add(invalidAction);
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
        List<LifecycleAction> actions = randomSubsetOf(VALID_COLD_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toList());
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete", "forcemerge", "shrink"));
            actions.add(invalidAction);
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
        List<LifecycleAction> actions = randomSubsetOf(VALID_DELETE_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toList());
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "rollover", "replicas", "forcemerge", "shrink"));
            actions.add(invalidAction);
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
