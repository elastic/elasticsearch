/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Represents the lifecycle of an index from creation to deletion. A
 * {@link TimeseriesLifecycleType} is made up of a set of {@link Phase}s which it will
 * move through. Soon we will constrain the phases using some kinda of lifecycle
 * type which will allow only particular {@link Phase}s to be defined, will
 * dictate the order in which the {@link Phase}s are executed and will define
 * which {@link LifecycleAction}s are allowed in each phase.
 */
public class TimeseriesLifecycleType implements LifecycleType {
    public static final TimeseriesLifecycleType INSTANCE = new TimeseriesLifecycleType();

    public static final String TYPE = "timeseries";

    static final String HOT_PHASE = "hot";
    static final String WARM_PHASE = "warm";
    static final String COLD_PHASE = "cold";
    static final String FROZEN_PHASE = "frozen";
    static final String DELETE_PHASE = "delete";
    public static final List<String> ORDERED_VALID_PHASES = List.of(HOT_PHASE, WARM_PHASE, COLD_PHASE, FROZEN_PHASE, DELETE_PHASE);

    public static final List<String> ORDERED_VALID_HOT_ACTIONS = Stream.of(
        SetPriorityAction.NAME,
        UnfollowAction.NAME,
        RolloverAction.NAME,
        ReadOnlyAction.NAME,
        DownsampleAction.NAME,
        ShrinkAction.NAME,
        ForceMergeAction.NAME,
        SearchableSnapshotAction.NAME
    ).filter(Objects::nonNull).toList();
    public static final List<String> ORDERED_VALID_WARM_ACTIONS = Stream.of(
        SetPriorityAction.NAME,
        UnfollowAction.NAME,
        ReadOnlyAction.NAME,
        AllocateAction.NAME,
        MigrateAction.NAME,
        DownsampleAction.NAME,
        ShrinkAction.NAME,
        ForceMergeAction.NAME
    ).filter(Objects::nonNull).toList();
    public static final List<String> ORDERED_VALID_COLD_ACTIONS = Stream.of(
        SetPriorityAction.NAME,
        UnfollowAction.NAME,
        ReadOnlyAction.NAME,
        SearchableSnapshotAction.NAME,
        AllocateAction.NAME,
        MigrateAction.NAME,
        DownsampleAction.NAME,
        FreezeAction.NAME
    ).filter(Objects::nonNull).toList();
    public static final List<String> ORDERED_VALID_FROZEN_ACTIONS = List.of(UnfollowAction.NAME, SearchableSnapshotAction.NAME);
    public static final List<String> ORDERED_VALID_DELETE_ACTIONS = List.of(WaitForSnapshotAction.NAME, DeleteAction.NAME);

    static final Set<String> VALID_HOT_ACTIONS = Sets.newHashSet(ORDERED_VALID_HOT_ACTIONS);
    static final Set<String> VALID_WARM_ACTIONS = Sets.newHashSet(ORDERED_VALID_WARM_ACTIONS);
    static final Set<String> VALID_COLD_ACTIONS = Sets.newHashSet(ORDERED_VALID_COLD_ACTIONS);
    static final Set<String> VALID_FROZEN_ACTIONS = Sets.newHashSet(ORDERED_VALID_FROZEN_ACTIONS);
    static final Set<String> VALID_DELETE_ACTIONS = Sets.newHashSet(ORDERED_VALID_DELETE_ACTIONS);

    private static final Map<String, Set<String>> ALLOWED_ACTIONS = Map.of(
        HOT_PHASE,
        VALID_HOT_ACTIONS,
        WARM_PHASE,
        VALID_WARM_ACTIONS,
        COLD_PHASE,
        VALID_COLD_ACTIONS,
        DELETE_PHASE,
        VALID_DELETE_ACTIONS,
        FROZEN_PHASE,
        VALID_FROZEN_ACTIONS
    );

    static final Set<String> HOT_ACTIONS_THAT_REQUIRE_ROLLOVER = Set.of(
        ReadOnlyAction.NAME,
        ShrinkAction.NAME,
        ForceMergeAction.NAME,
        DownsampleAction.NAME,
        SearchableSnapshotAction.NAME
    );
    // Set of actions that cannot be defined (executed) after the managed index has been mounted as searchable snapshot.
    // It's ordered to produce consistent error messages which can be unit tested.
    public static final Set<String> ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT = Collections.unmodifiableSet(
        new LinkedHashSet<>(Arrays.asList(ForceMergeAction.NAME, FreezeAction.NAME, ShrinkAction.NAME, DownsampleAction.NAME))
    );

    private TimeseriesLifecycleType() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    public List<Phase> getOrderedPhases(Map<String, Phase> phases) {
        List<Phase> orderedPhases = new ArrayList<>(ORDERED_VALID_PHASES.size());
        for (String phaseName : ORDERED_VALID_PHASES) {
            Phase phase = phases.get(phaseName);
            if (phase != null) {
                Map<String, LifecycleAction> actions = phase.getActions();
                if (actions.containsKey(UnfollowAction.NAME) == false
                    && (actions.containsKey(RolloverAction.NAME)
                        || actions.containsKey(ShrinkAction.NAME)
                        || actions.containsKey(SearchableSnapshotAction.NAME))) {
                    Map<String, LifecycleAction> actionMap = new HashMap<>(phase.getActions());
                    actionMap.put(UnfollowAction.NAME, UnfollowAction.INSTANCE);
                    phase = new Phase(phase.getName(), phase.getMinimumAge(), actionMap);
                }

                if (shouldInjectMigrateStepForPhase(phase)) {
                    Map<String, LifecycleAction> actionMap = new HashMap<>(phase.getActions());
                    actionMap.put(MigrateAction.NAME, MigrateAction.ENABLED);
                    phase = new Phase(phase.getName(), phase.getMinimumAge(), actionMap);
                }

                orderedPhases.add(phase);
            }
        }
        return orderedPhases;
    }

    public static boolean shouldInjectMigrateStepForPhase(Phase phase) {
        if (ALLOWED_ACTIONS.containsKey(phase.getName()) == false) {
            return false;
        }

        // searchable snapshots automatically set their own allocation rules, no need to configure them with a migrate step.
        if (phase.getActions().get(SearchableSnapshotAction.NAME) != null) {
            return false;
        }

        // do not inject if MigrateAction is not supported for this phase (such as hot, frozen, delete phase)
        if (ALLOWED_ACTIONS.get(phase.getName()).contains(MigrateAction.NAME) == false) {
            return false;
        }

        // if the user configured the {@link MigrateAction} already we won't automatically configure it
        if (phase.getActions().get(MigrateAction.NAME) != null) {
            return false;
        }

        return true;
    }

    @Override
    public String getNextPhaseName(String currentPhaseName, Map<String, Phase> phases) {
        int index = ORDERED_VALID_PHASES.indexOf(currentPhaseName);
        if (index < 0 && "new".equals(currentPhaseName) == false) {
            throw new IllegalArgumentException("[" + currentPhaseName + "] is not a valid phase for lifecycle type [" + TYPE + "]");
        } else {
            // Find the next phase after `index` that exists in `phases` and return it
            while (++index < ORDERED_VALID_PHASES.size()) {
                String phaseName = ORDERED_VALID_PHASES.get(index);
                if (phases.containsKey(phaseName)) {
                    return phaseName;
                }
            }
            // if we have exhausted VALID_PHASES and haven't found a matching
            // phase in `phases` return null indicating there is no next phase
            // available
            return null;
        }
    }

    public String getPreviousPhaseName(String currentPhaseName, Map<String, Phase> phases) {
        if ("new".equals(currentPhaseName)) {
            return null;
        }
        int index = ORDERED_VALID_PHASES.indexOf(currentPhaseName);
        if (index < 0) {
            throw new IllegalArgumentException("[" + currentPhaseName + "] is not a valid phase for lifecycle type [" + TYPE + "]");
        } else {
            // Find the previous phase before `index` that exists in `phases` and return it
            while (--index >= 0) {
                String phaseName = ORDERED_VALID_PHASES.get(index);
                if (phases.containsKey(phaseName)) {
                    return phaseName;
                }
            }
            // if we have exhausted VALID_PHASES and haven't found a matching
            // phase in `phases` return null indicating there is no previous phase
            // available
            return null;
        }
    }

    public List<LifecycleAction> getOrderedActions(Phase phase) {
        Map<String, LifecycleAction> actions = phase.getActions();
        return switch (phase.getName()) {
            case HOT_PHASE -> ORDERED_VALID_HOT_ACTIONS.stream().map(actions::get).filter(Objects::nonNull).collect(toList());
            case WARM_PHASE -> ORDERED_VALID_WARM_ACTIONS.stream().map(actions::get).filter(Objects::nonNull).collect(toList());
            case COLD_PHASE -> ORDERED_VALID_COLD_ACTIONS.stream().map(actions::get).filter(Objects::nonNull).collect(toList());
            case FROZEN_PHASE -> ORDERED_VALID_FROZEN_ACTIONS.stream().map(actions::get).filter(Objects::nonNull).collect(toList());
            case DELETE_PHASE -> ORDERED_VALID_DELETE_ACTIONS.stream().map(actions::get).filter(Objects::nonNull).collect(toList());
            default -> throw new IllegalArgumentException("lifecycle type [" + TYPE + "] does not support phase [" + phase.getName() + "]");
        };
    }

    @Override
    public String getNextActionName(String currentActionName, Phase phase) {
        List<String> orderedActionNames = switch (phase.getName()) {
            case HOT_PHASE -> ORDERED_VALID_HOT_ACTIONS;
            case WARM_PHASE -> ORDERED_VALID_WARM_ACTIONS;
            case COLD_PHASE -> ORDERED_VALID_COLD_ACTIONS;
            case FROZEN_PHASE -> ORDERED_VALID_FROZEN_ACTIONS;
            case DELETE_PHASE -> ORDERED_VALID_DELETE_ACTIONS;
            default -> throw new IllegalArgumentException("lifecycle type [" + TYPE + "] does not support phase [" + phase.getName() + "]");
        };

        int index = orderedActionNames.indexOf(currentActionName);
        if (index < 0) {
            throw new IllegalArgumentException(
                "[" + currentActionName + "] is not a valid action for phase [" + phase.getName() + "] in lifecycle type [" + TYPE + "]"
            );
        } else {
            // Find the next action after `index` that exists in the phase and return it
            while (++index < orderedActionNames.size()) {
                String actionName = orderedActionNames.get(index);
                if (phase.getActions().containsKey(actionName)) {
                    return actionName;
                }
            }
            // if we have exhausted `validActions` and haven't found a matching
            // action in the Phase return null indicating there is no next
            // action available
            return null;
        }
    }

    @Override
    public void validate(Collection<Phase> phases) {
        phases.forEach(phase -> {
            if (ALLOWED_ACTIONS.containsKey(phase.getName()) == false) {
                throw new IllegalArgumentException("Timeseries lifecycle does not support phase [" + phase.getName() + "]");
            }
            phase.getActions().forEach((actionName, action) -> {
                if (ALLOWED_ACTIONS.get(phase.getName()).contains(actionName) == false) {
                    throw new IllegalArgumentException(
                        "invalid action [" + actionName + "] " + "defined in phase [" + phase.getName() + "]"
                    );
                }
            });
        });

        // Check for actions in the hot phase that require a rollover
        String invalidHotPhaseActions = phases.stream()
            // Is there a hot phase
            .filter(phase -> HOT_PHASE.equals(phase.getName()))
            // that does *not* contain the 'rollover' action
            .filter(phase -> phase.getActions().containsKey(RolloverAction.NAME) == false)
            // but that does have actions that require a rollover action?
            .flatMap(phase -> Sets.intersection(phase.getActions().keySet(), HOT_ACTIONS_THAT_REQUIRE_ROLLOVER).stream())
            .collect(Collectors.joining(", "));
        if (Strings.hasText(invalidHotPhaseActions)) {
            throw new IllegalArgumentException(
                "the ["
                    + invalidHotPhaseActions
                    + "] action(s) may not be used in the ["
                    + HOT_PHASE
                    + "] phase without an accompanying ["
                    + RolloverAction.NAME
                    + "] action"
            );
        }

        // look for phases that have the migrate action enabled and also specify allocation rules via the AllocateAction
        String phasesWithConflictingMigrationActions = phases.stream()
            .filter(
                phase -> phase.getActions().containsKey(MigrateAction.NAME)
                    && ((MigrateAction) phase.getActions().get(MigrateAction.NAME)).isEnabled()
                    && phase.getActions().containsKey(AllocateAction.NAME)
                    && definesAllocationRules((AllocateAction) phase.getActions().get(AllocateAction.NAME))
            )
            .map(Phase::getName)
            .collect(Collectors.joining(","));
        if (Strings.hasText(phasesWithConflictingMigrationActions)) {
            throw new IllegalArgumentException(
                "phases ["
                    + phasesWithConflictingMigrationActions
                    + "] specify an enabled "
                    + MigrateAction.NAME
                    + " action and an "
                    + AllocateAction.NAME
                    + " action with allocation rules. specify only a single "
                    + "data migration in each phase"
            );
        }

        validateActionsFollowingSearchableSnapshot(phases);
        validateAllSearchableSnapshotActionsUseSameRepository(phases);
        validateFrozenPhaseHasSearchableSnapshotAction(phases);
        validateDownsamplingIntervals(phases);
    }

    static void validateActionsFollowingSearchableSnapshot(Collection<Phase> phases) {
        // invalid configurations can occur if searchable_snapshot is defined in the `hot` phase, with subsequent invalid actions
        // being defined in the warm/cold/frozen phases, or if it is defined in the `cold` phase with subsequent invalid actions
        // being defined in the frozen phase

        Optional<Phase> hotPhaseWithSearchableSnapshot = phases.stream()
            .filter(phase -> phase.getName().equals(HOT_PHASE))
            .filter(phase -> phase.getActions().containsKey(SearchableSnapshotAction.NAME))
            .findAny();

        final List<Phase> phasesFollowingSearchableSnapshot = new ArrayList<>(phases.size());
        if (hotPhaseWithSearchableSnapshot.isPresent()) {
            for (Phase phase : phases) {
                if (phase.getName().equals(HOT_PHASE) == false) {
                    phasesFollowingSearchableSnapshot.add(phase);
                }
            }
        } else {
            // let's see if the cold phase defines `searchable_snapshot`
            Optional<Phase> coldPhaseWithSearchableSnapshot = phases.stream()
                .filter(phase -> phase.getName().equals(COLD_PHASE))
                .filter(phase -> phase.getActions().containsKey(SearchableSnapshotAction.NAME))
                .findAny();
            if (coldPhaseWithSearchableSnapshot.isPresent()) {
                for (Phase phase : phases) {
                    if (phase.getName().equals(FROZEN_PHASE)) {
                        phasesFollowingSearchableSnapshot.add(phase);
                        break;
                    }
                }
            }
        }

        final String phasesDefiningIllegalActions = phasesFollowingSearchableSnapshot.stream()
            // filter the phases that define illegal actions
            .filter(phase -> Collections.disjoint(ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT, phase.getActions().keySet()) == false)
            .map(Phase::getName)
            .collect(Collectors.joining(","));
        if (Strings.hasText(phasesDefiningIllegalActions)) {
            throw new IllegalArgumentException(
                "phases ["
                    + phasesDefiningIllegalActions
                    + "] define one or more of "
                    + ACTIONS_CANNOT_FOLLOW_SEARCHABLE_SNAPSHOT
                    + " actions which are not allowed after a "
                    + "managed index is mounted as a searchable snapshot"
            );
        }
    }

    static void validateAllSearchableSnapshotActionsUseSameRepository(Collection<Phase> phases) {
        Set<String> allRepos = phases.stream()
            .flatMap(phase -> phase.getActions().entrySet().stream())
            .filter(e -> e.getKey().equals(SearchableSnapshotAction.NAME))
            .map(Map.Entry::getValue)
            .map(action -> (SearchableSnapshotAction) action)
            .map(SearchableSnapshotAction::getSnapshotRepository)
            .collect(Collectors.toSet());

        if (allRepos.size() > 1) {
            throw new IllegalArgumentException(
                "policy specifies ["
                    + SearchableSnapshotAction.NAME
                    + "] action multiple times with differing repositories "
                    + allRepos
                    + ", the same repository must be used for all searchable snapshot actions"
            );
        }
    }

    /**
     * Validates that phases don't configure a min_age that is smaller than a previous phase (which can be confusing to users)
     */
    public static String validateMonotonicallyIncreasingPhaseTimings(Collection<Phase> phases) {
        List<String> errors = new ArrayList<>();
        Set<String> invalidPhases = new HashSet<>();

        // Loop through all phases in order, for each phase with a min_age
        // configured, look at all the future phases to see if their ages are
        // >= the configured age. A min_age of 0 means that the age was not
        // configured, so we don't validate it.
        for (int i = 0; i < ORDERED_VALID_PHASES.size(); i++) {
            String phaseName = ORDERED_VALID_PHASES.get(i);
            // Check if this phase is present with a configured min_age
            Optional<Phase> maybePhase = phases.stream()
                .filter(p -> phaseName.equals(p.getName()))
                .filter(p -> p.getMinimumAge() != null && p.getMinimumAge().equals(TimeValue.ZERO) == false)
                .findFirst();

            if (maybePhase.isPresent()) {
                Phase phase = maybePhase.get();
                // We only consider a phase bad once, otherwise we can duplicate
                // errors, so we keep track of the invalid phases we've seen and
                // ignore them if they come around again.
                if (invalidPhases.contains(phase.getName())) {
                    continue;
                }
                TimeValue phaseMinAge = phase.getMinimumAge();
                Set<String> followingPhases = new HashSet<>(ORDERED_VALID_PHASES.subList(i + 1, ORDERED_VALID_PHASES.size()));
                Set<Phase> phasesWithBadAges = phases.stream()
                    .filter(p -> followingPhases.contains(p.getName()))
                    .filter(p -> p.getMinimumAge() != null && p.getMinimumAge().equals(TimeValue.ZERO) == false)
                    .filter(p -> p.getMinimumAge().compareTo(phaseMinAge) < 0)
                    .collect(Collectors.toSet());
                if (phasesWithBadAges.size() > 0) {
                    phasesWithBadAges.forEach(p -> invalidPhases.add(p.getName()));

                    // build an error message string
                    Iterator<Phase> it = phasesWithBadAges.iterator();
                    Phase badPhase = it.next();

                    String error = "Your policy is configured to run the "
                        + badPhase.getName()
                        + " phase (min_age: "
                        + badPhase.getMinimumAge()
                        + ")";

                    if (phasesWithBadAges.size() > 1) {
                        while (it.hasNext()) {
                            badPhase = it.next();
                            error = error + ", the " + badPhase.getName() + " phase (min_age: " + badPhase.getMinimumAge() + ")";
                        }
                        // if multiple phases are cited replace last occurrence of "," with " and"
                        StringBuilder builder = new StringBuilder();
                        int last_comma_index = error.lastIndexOf(",");
                        builder.append(error, 0, last_comma_index);
                        builder.append(" and");
                        builder.append(error.substring(last_comma_index + 1));
                        error = builder.toString();
                    }
                    error = error
                        + " before the "
                        + phaseName
                        + " phase (min_age: "
                        + phase.getMinimumAge()
                        + "). You should change the phase timing so that the phases will execute in the order of hot, warm, then cold.";

                    errors.add(error);
                }
            }
        }

        // If we found any invalid phase timings, concatenate their messages and return the message
        return Strings.collectionToCommaDelimitedString(errors);
    }

    /**
     * Require that the "frozen" phase configured in a policy has a searchable snapshot action.
     */
    static void validateFrozenPhaseHasSearchableSnapshotAction(Collection<Phase> phases) {
        Optional<Phase> maybeFrozenPhase = phases.stream().filter(p -> FROZEN_PHASE.equals(p.getName())).findFirst();

        maybeFrozenPhase.ifPresent(p -> {
            if (p.getActions().containsKey(SearchableSnapshotAction.NAME) == false) {
                throw new IllegalArgumentException(
                    "policy specifies the ["
                        + FROZEN_PHASE
                        + "] phase without a corresponding ["
                        + SearchableSnapshotAction.NAME
                        + "] action, but a searchable snapshot action is required in the frozen phase"
                );
            }
        });
    }

    /**
     * Add validations if there are multiple downsample actions on different phases. The rules that we
     * enforce are the following:
     *   - The latter interval must be greater than the previous interval
     *   - The latter interval must be a multiple of the previous interval
     */
    static void validateDownsamplingIntervals(Collection<Phase> phases) {
        Map<String, Phase> phasesWithDownsamplingActions = phases.stream()
            .filter(phase -> phase.getActions().containsKey(DownsampleAction.NAME))
            .collect(Collectors.toMap(Phase::getName, Function.identity()));

        if (phasesWithDownsamplingActions.size() < 2) {
            // Interval validations must be executed when there are at least two downsample actions, otherwise return
            return;
        }

        // Order phases and extract the downsample action instances per phase
        List<Phase> orderedPhases = INSTANCE.getOrderedPhases(phasesWithDownsamplingActions);
        var downsampleActions = orderedPhases.stream()
            .map(phase -> Tuple.tuple(phase.getName(), (DownsampleAction) phase.getActions().get(DownsampleAction.NAME)))
            .toList(); // Returns a list of tuples (phase name, downsample action)

        var firstDownsample = downsampleActions.get(0);
        for (int i = 1; i < downsampleActions.size(); i++) {
            var secondDownsample = downsampleActions.get(i);
            var firstInterval = firstDownsample.v2().fixedInterval();
            var secondInterval = secondDownsample.v2().fixedInterval();
            long firstMillis = firstInterval.estimateMillis();
            long secondMillis = secondInterval.estimateMillis();
            if (firstMillis >= secondMillis) {
                // The later interval must be greater than the previous interval
                throw new IllegalArgumentException(
                    "Downsampling interval ["
                        + secondInterval
                        + "] for phase ["
                        + secondDownsample.v1()
                        + "] must be greater than the interval ["
                        + firstInterval
                        + "] for phase ["
                        + firstDownsample.v1()
                        + "]"
                );
            } else if (secondMillis % firstMillis != 0) {
                // Downsampling interval must be a multiple of the source interval
                throw new IllegalArgumentException(
                    "Downsampling interval ["
                        + secondInterval
                        + "] for phase ["
                        + secondDownsample.v1()
                        + "] must be a multiple of the interval ["
                        + firstInterval
                        + "] for phase ["
                        + firstDownsample.v1()
                        + "]"
                );
            }
            firstDownsample = secondDownsample;
        }
    }

    private static boolean definesAllocationRules(AllocateAction action) {
        return action.getRequire().isEmpty() == false || action.getInclude().isEmpty() == false || action.getExclude().isEmpty() == false;
    }
}
