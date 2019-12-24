/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class LifecyclePolicyTests extends AbstractXContentTestCase<LifecyclePolicy> {
    private static final Set<String> VALID_HOT_ACTIONS = Sets.newHashSet(UnfollowAction.NAME, SetPriorityAction.NAME, RolloverAction.NAME);
    private static final Set<String> VALID_WARM_ACTIONS = Sets.newHashSet(UnfollowAction.NAME, SetPriorityAction.NAME, AllocateAction.NAME,
        ForceMergeAction.NAME, ReadOnlyAction.NAME, ShrinkAction.NAME);
    private static final Set<String> VALID_COLD_ACTIONS = Sets.newHashSet(UnfollowAction.NAME, SetPriorityAction.NAME, AllocateAction.NAME,
        FreezeAction.NAME);
    private static final Set<String> VALID_DELETE_ACTIONS = Sets.newHashSet(DeleteAction.NAME);

    private String lifecycleName;

    @Override
    protected LifecyclePolicy doParseInstance(XContentParser parser) {
        return LifecyclePolicy.parse(parser, lifecycleName);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // these items all have some specific parsing that does not allow them to have additional objects within them.
        return (field) -> field.contains("allocate.") || field.equals("phases") || field.endsWith("actions");
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(Arrays.asList(
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
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
        return createRandomPolicy(lifecycleName);
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
            Exception e = expectThrows(IllegalArgumentException.class, () -> new LifecyclePolicy(lifecycleName, phases));
            assertThat(e.getMessage(), equalTo("Lifecycle does not support phase [" + phaseName + "]"));
        } else {
            new LifecyclePolicy(lifecycleName, phases);
        }
    }

    public void testValidateHotPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_HOT_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "forcemerge", "delete", "shrink"));
            actions.put(invalidAction.getName(), invalidAction);
        }
        Map<String, Phase> hotPhase = Collections.singletonMap("hot",
            new Phase("hot", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> new LifecyclePolicy(lifecycleName, hotPhase));
            assertThat(e.getMessage(),
                equalTo("invalid action [" + invalidAction.getName() + "] defined in phase [hot]"));
        } else {
            new LifecyclePolicy(lifecycleName, hotPhase);
        }
    }

    public void testValidateWarmPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_WARM_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete"));
            actions.put(invalidAction.getName(), invalidAction);
        }
        Map<String, Phase> warmPhase = Collections.singletonMap("warm",
            new Phase("warm", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> new LifecyclePolicy(lifecycleName, warmPhase));
            assertThat(e.getMessage(),
                equalTo("invalid action [" + invalidAction.getName() + "] defined in phase [warm]"));
        } else {
            new LifecyclePolicy(lifecycleName, warmPhase);
        }
    }

    public void testValidateColdPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_COLD_ACTIONS)
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete", "forcemerge", "shrink"));
            actions.put(invalidAction.getName(), invalidAction);
        }
        Map<String, Phase> coldPhase = Collections.singletonMap("cold",
            new Phase("cold", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> new LifecyclePolicy(lifecycleName, coldPhase));
            assertThat(e.getMessage(),
                equalTo("invalid action [" + invalidAction.getName() + "] defined in phase [cold]"));
        } else {
            new LifecyclePolicy(lifecycleName, coldPhase);
        }
    }

    public void testValidateDeletePhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = VALID_DELETE_ACTIONS
            .stream().map(this::getTestAction).collect(Collectors.toMap(LifecycleAction::getName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "rollover", "forcemerge", "shrink"));
            actions.put(invalidAction.getName(), invalidAction);
        }
        Map<String, Phase> deletePhase = Collections.singletonMap("delete",
            new Phase("delete", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> new LifecyclePolicy(lifecycleName, deletePhase));
            assertThat(e.getMessage(),
                equalTo("invalid action [" + invalidAction.getName() + "] defined in phase [delete]"));
        } else {
            new LifecyclePolicy(lifecycleName, deletePhase);
        }
    }

    public static LifecyclePolicy createRandomPolicy(String lifecycleName) {
        List<String> phaseNames = randomSubsetOf(Arrays.asList("hot", "warm", "cold", "delete"));
        Map<String, Phase> phases = new HashMap<>(phaseNames.size());
        Function<String, Set<String>> validActions = (phase) ->  {
            switch (phase) {
                case "hot":
                    return VALID_HOT_ACTIONS;
                case "warm":
                    return VALID_WARM_ACTIONS;
                case "cold":
                    return VALID_COLD_ACTIONS;
                case "delete":
                    return VALID_DELETE_ACTIONS;
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
        return new LifecyclePolicy(lifecycleName, phases);
    }

    private LifecycleAction getTestAction(String actionName) {
        switch (actionName) {
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
            case FreezeAction.NAME:
                return new FreezeAction();
            case SetPriorityAction.NAME:
                return SetPriorityActionTests.randomInstance();
            case UnfollowAction.NAME:
                return new UnfollowAction();
            default:
                throw new IllegalArgumentException("unsupported phase action [" + actionName + "]");
        }
    }
}
