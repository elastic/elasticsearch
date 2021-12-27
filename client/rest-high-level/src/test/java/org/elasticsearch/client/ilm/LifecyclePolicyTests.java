/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class LifecyclePolicyTests extends AbstractXContentTestCase<LifecyclePolicy> {
    private static final Set<String> VALID_HOT_ACTIONS = new HashSet<>(TimeseriesLifecycleType.ORDERED_VALID_HOT_ACTIONS);
    private static final Set<String> VALID_WARM_ACTIONS = new HashSet<>(TimeseriesLifecycleType.ORDERED_VALID_WARM_ACTIONS);
    private static final Set<String> VALID_COLD_ACTIONS = new HashSet<>(TimeseriesLifecycleType.ORDERED_VALID_COLD_ACTIONS);
    private static final Set<String> VALID_FROZEN_ACTIONS = new HashSet<>(TimeseriesLifecycleType.ORDERED_VALID_FROZEN_ACTIONS);
    private static final Set<String> VALID_DELETE_ACTIONS = new HashSet<>(TimeseriesLifecycleType.ORDERED_VALID_DELETE_ACTIONS);

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
        entries.addAll(
            Arrays.asList(
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(WaitForSnapshotAction.NAME),
                    WaitForSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(SearchableSnapshotAction.NAME),
                    SearchableSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MigrateAction.NAME), MigrateAction::parse)
            )
        );
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
        Map<String, Phase> phases = Collections.singletonMap(
            phaseName,
            new Phase(
                phaseName,
                TimeValue.ZERO,
                phaseName.equals("delete") ? Collections.singletonMap(DeleteAction.NAME, new DeleteAction()) : Collections.emptyMap()
            )
        );
        if (invalid) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> new LifecyclePolicy(lifecycleName, phases));
            assertThat(e.getMessage(), equalTo("Lifecycle does not support phase [" + phaseName + "]"));
        } else {
            new LifecyclePolicy(lifecycleName, phases);
        }
    }

    public void testValidateHotPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_HOT_ACTIONS).stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "migrate", "delete"));
            actions.put(invalidAction.getName(), invalidAction);
        }
        Map<String, Phase> hotPhase = Collections.singletonMap("hot", new Phase("hot", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(
                IllegalArgumentException.class,
                "expected " + invalidAction + " to throw but it didn't",
                () -> new LifecyclePolicy(lifecycleName, hotPhase)
            );
            assertThat(e.getMessage(), equalTo("invalid action [" + invalidAction.getName() + "] defined in phase [hot]"));
        } else {
            new LifecyclePolicy(lifecycleName, hotPhase);
        }
    }

    public void testValidateWarmPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_WARM_ACTIONS).stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete", "searchable_snapshot"));
            actions.put(invalidAction.getName(), invalidAction);
        }
        Map<String, Phase> warmPhase = Collections.singletonMap("warm", new Phase("warm", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> new LifecyclePolicy(lifecycleName, warmPhase));
            assertThat(e.getMessage(), equalTo("invalid action [" + invalidAction.getName() + "] defined in phase [warm]"));
        } else {
            new LifecyclePolicy(lifecycleName, warmPhase);
        }
    }

    public void testValidateColdPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_COLD_ACTIONS).stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("rollover", "delete", "forcemerge", "shrink"));
            actions.put(invalidAction.getName(), invalidAction);
        }
        Map<String, Phase> coldPhase = Collections.singletonMap("cold", new Phase("cold", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> new LifecyclePolicy(lifecycleName, coldPhase));
            assertThat(e.getMessage(), equalTo("invalid action [" + invalidAction.getName() + "] defined in phase [cold]"));
        } else {
            new LifecyclePolicy(lifecycleName, coldPhase);
        }
    }

    public void testValidateFrozenPhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = randomSubsetOf(VALID_FROZEN_ACTIONS).stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "rollover", "delete", "forcemerge", "shrink", "readonly"));
            actions.put(invalidAction.getName(), invalidAction);
        }
        Map<String, Phase> coldPhase = Collections.singletonMap("cold", new Phase("frozen", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> new LifecyclePolicy(lifecycleName, coldPhase));
            assertThat(e.getMessage(), equalTo("invalid action [" + invalidAction.getName() + "] defined in phase [frozen]"));
        } else {
            new LifecyclePolicy(lifecycleName, coldPhase);
        }
    }

    public void testValidateDeletePhase() {
        LifecycleAction invalidAction = null;
        Map<String, LifecycleAction> actions = VALID_DELETE_ACTIONS.stream()
            .map(this::getTestAction)
            .collect(Collectors.toMap(LifecycleAction::getName, Function.identity()));
        if (randomBoolean()) {
            invalidAction = getTestAction(randomFrom("allocate", "rollover", "forcemerge", "shrink"));
            actions.put(invalidAction.getName(), invalidAction);
        }
        Map<String, Phase> deletePhase = Collections.singletonMap("delete", new Phase("delete", TimeValue.ZERO, actions));

        if (invalidAction != null) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> new LifecyclePolicy(lifecycleName, deletePhase));
            assertThat(e.getMessage(), equalTo("invalid action [" + invalidAction.getName() + "] defined in phase [delete]"));
        } else {
            new LifecyclePolicy(lifecycleName, deletePhase);
        }
    }

    public void testValidateEmptyDeletePhase() {
        Map<String, LifecycleAction> actions = new HashMap<>();

        Phase delete = new Phase("delete", TimeValue.ZERO, actions);
        Map<String, Phase> phases = Collections.singletonMap("delete", delete);

        Exception e = expectThrows(IllegalArgumentException.class, () -> new LifecyclePolicy(lifecycleName, phases));
        assertThat(e.getMessage(), equalTo("phase [" + delete.getName() + "] must define actions"));
    }

    public static LifecyclePolicy createRandomPolicy(String lifecycleName) {
        List<String> phaseNames = Arrays.asList("hot", "warm", "cold", "delete");
        Map<String, Phase> phases = new HashMap<>(phaseNames.size());
        Function<String, Set<String>> validActions = (phase) -> {
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
            }
        };
        Function<String, Boolean> allowEmptyActions = (phase) -> {
            switch (phase) {
                case "hot":
                case "warm":
                case "cold":
                    return true;
                case "delete":
                    return false;
                default:
                    throw new IllegalArgumentException("invalid phase [" + phase + "]");
            }
        };
        Function<String, LifecycleAction> randomAction = (action) -> {
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
                case WaitForSnapshotAction.NAME:
                    return WaitForSnapshotActionTests.randomInstance();
                case SetPriorityAction.NAME:
                    return SetPriorityActionTests.randomInstance();
                case UnfollowAction.NAME:
                    return new UnfollowAction();
                case SearchableSnapshotAction.NAME:
                    return new SearchableSnapshotAction("repo", randomBoolean());
                case MigrateAction.NAME:
                    return new MigrateAction(randomBoolean());
                default:
                    throw new IllegalArgumentException("invalid action [" + action + "]");
            }
        };
        TimeValue prev = null;
        boolean searchableSnapshotSeen = false;
        for (String phase : phaseNames) {
            TimeValue after = prev == null
                ? TimeValue.parseTimeValue(randomTimeValue(0, 10000, "s", "m", "h", "d"), "test_after")
                : TimeValue.timeValueSeconds(prev.seconds() + randomIntBetween(60, 600));
            prev = after;
            Map<String, LifecycleAction> actions = new HashMap<>();
            List<String> actionNames;
            if (allowEmptyActions.apply(phase)) {
                actionNames = randomSubsetOf(validActions.apply(phase));
            } else {
                actionNames = randomSubsetOf(randomIntBetween(1, validActions.apply(phase).size()), validActions.apply(phase));
            }
            if ("hot".equals(phase)) {
                actions.put(
                    RolloverAction.NAME,
                    new RolloverAction(null, new ByteSizeValue(randomNonNegativeLong()), null, randomNonNegativeLong())
                );
            }
            if (searchableSnapshotSeen || actionNames.contains(SearchableSnapshotAction.NAME)) {
                searchableSnapshotSeen = true;
                // let's make sure phases don't configure actions that conflict with the `searchable_snapshot` action
                actionNames.removeAll(TimeseriesLifecycleType.ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT);
            }
            if (actionNames.contains(MigrateAction.NAME)) {
                actionNames.remove(AllocateAction.NAME);
            }
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
            case WaitForSnapshotAction.NAME:
                return WaitForSnapshotActionTests.randomInstance();
            case SetPriorityAction.NAME:
                return SetPriorityActionTests.randomInstance();
            case SearchableSnapshotAction.NAME:
                return SearchableSnapshotActionTests.randomInstance();
            case UnfollowAction.NAME:
                return new UnfollowAction();
            case MigrateAction.NAME:
                return new MigrateAction(randomBoolean());
            default:
                throw new IllegalArgumentException("unsupported phase action [" + actionName + "]");
        }
    }
}
