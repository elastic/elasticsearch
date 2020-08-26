/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
    static final List<String> VALID_PHASES = Arrays.asList(HOT_PHASE, WARM_PHASE, COLD_PHASE, FROZEN_PHASE, DELETE_PHASE);
    static final List<String> ORDERED_VALID_HOT_ACTIONS = Arrays.asList(SetPriorityAction.NAME, UnfollowAction.NAME, RolloverAction.NAME,
        ForceMergeAction.NAME);
    static final List<String> ORDERED_VALID_WARM_ACTIONS = Arrays.asList(SetPriorityAction.NAME, UnfollowAction.NAME, ReadOnlyAction.NAME,
        AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME);
    static final List<String> ORDERED_VALID_COLD_ACTIONS = Arrays.asList(SetPriorityAction.NAME, UnfollowAction.NAME, AllocateAction.NAME,
        FreezeAction.NAME, SearchableSnapshotAction.NAME);
    static final List<String> ORDERED_VALID_FROZEN_ACTIONS = Arrays.asList(SetPriorityAction.NAME, UnfollowAction.NAME, AllocateAction.NAME,
        FreezeAction.NAME, SearchableSnapshotAction.NAME);
    static final List<String> ORDERED_VALID_DELETE_ACTIONS = Arrays.asList(WaitForSnapshotAction.NAME, DeleteAction.NAME);
    static final Set<String> VALID_HOT_ACTIONS = Sets.newHashSet(ORDERED_VALID_HOT_ACTIONS);
    static final Set<String> VALID_WARM_ACTIONS = Sets.newHashSet(ORDERED_VALID_WARM_ACTIONS);
    static final Set<String> VALID_COLD_ACTIONS = Sets.newHashSet(ORDERED_VALID_COLD_ACTIONS);
    static final Set<String> VALID_FROZEN_ACTIONS = Sets.newHashSet(ORDERED_VALID_FROZEN_ACTIONS);
    static final Set<String> VALID_DELETE_ACTIONS = Sets.newHashSet(ORDERED_VALID_DELETE_ACTIONS);
    private static Map<String, Set<String>> ALLOWED_ACTIONS = new HashMap<>();

    static {
        ALLOWED_ACTIONS.put(HOT_PHASE, VALID_HOT_ACTIONS);
        ALLOWED_ACTIONS.put(WARM_PHASE, VALID_WARM_ACTIONS);
        ALLOWED_ACTIONS.put(COLD_PHASE, VALID_COLD_ACTIONS);
        ALLOWED_ACTIONS.put(FROZEN_PHASE, VALID_FROZEN_ACTIONS);
        ALLOWED_ACTIONS.put(DELETE_PHASE, VALID_DELETE_ACTIONS);
    }

    private TimeseriesLifecycleType() {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    public List<Phase> getOrderedPhases(Map<String, Phase> phases) {
        List<Phase> orderedPhases = new ArrayList<>(VALID_PHASES.size());
        for (String phaseName : VALID_PHASES) {
            Phase phase = phases.get(phaseName);
            if (phase != null) {
                Map<String, LifecycleAction> actions = phase.getActions();
                if (actions.containsKey(UnfollowAction.NAME) == false &&
                    (actions.containsKey(RolloverAction.NAME) || actions.containsKey(ShrinkAction.NAME) ||
                        actions.containsKey(SearchableSnapshotAction.NAME))) {
                    Map<String, LifecycleAction> actionMap = new HashMap<>(phase.getActions());
                    actionMap.put(UnfollowAction.NAME, new UnfollowAction());
                    phase = new Phase(phase.getName(), phase.getMinimumAge(), actionMap);
                }
                orderedPhases.add(phase);
            }
        }
        return orderedPhases;
    }

    @Override
    public String getNextPhaseName(String currentPhaseName, Map<String, Phase> phases) {
        int index = VALID_PHASES.indexOf(currentPhaseName);
        if (index < 0 && "new".equals(currentPhaseName) == false) {
            throw new IllegalArgumentException("[" + currentPhaseName + "] is not a valid phase for lifecycle type [" + TYPE + "]");
        } else {
            // Find the next phase after `index` that exists in `phases` and return it
            while (++index < VALID_PHASES.size()) {
                String phaseName = VALID_PHASES.get(index);
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
        int index = VALID_PHASES.indexOf(currentPhaseName);
        if (index < 0) {
            throw new IllegalArgumentException("[" + currentPhaseName + "] is not a valid phase for lifecycle type [" + TYPE + "]");
        } else {
            // Find the previous phase before `index` that exists in `phases` and return it
            while (--index >=0) {
                String phaseName = VALID_PHASES.get(index);
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
        switch (phase.getName()) {
            case HOT_PHASE:
                return ORDERED_VALID_HOT_ACTIONS.stream().map(a -> actions.getOrDefault(a, null))
                    .filter(Objects::nonNull).collect(Collectors.toList());
            case WARM_PHASE:
                return ORDERED_VALID_WARM_ACTIONS.stream() .map(a -> actions.getOrDefault(a, null))
                    .filter(Objects::nonNull).collect(Collectors.toList());
            case COLD_PHASE:
                return ORDERED_VALID_COLD_ACTIONS.stream().map(a -> actions.getOrDefault(a, null))
                    .filter(Objects::nonNull).collect(Collectors.toList());
            case FROZEN_PHASE:
                return ORDERED_VALID_FROZEN_ACTIONS.stream().map(a -> actions.getOrDefault(a, null))
                    .filter(Objects::nonNull).collect(Collectors.toList());
            case DELETE_PHASE:
                return ORDERED_VALID_DELETE_ACTIONS.stream().map(a -> actions.getOrDefault(a, null))
                    .filter(Objects::nonNull).collect(Collectors.toList());
            default:
                throw new IllegalArgumentException("lifecycle type[" + TYPE + "] does not support phase[" + phase.getName() + "]");
        }
    }

    @Override
    public String getNextActionName(String currentActionName, Phase phase) {
        List<String> orderedActionNames;
        switch (phase.getName()) {
        case HOT_PHASE:
            orderedActionNames = ORDERED_VALID_HOT_ACTIONS;
            break;
        case WARM_PHASE:
            orderedActionNames = ORDERED_VALID_WARM_ACTIONS;
            break;
        case COLD_PHASE:
            orderedActionNames = ORDERED_VALID_COLD_ACTIONS;
            break;
        case FROZEN_PHASE:
            orderedActionNames = ORDERED_VALID_FROZEN_ACTIONS;
            break;
        case DELETE_PHASE:
            orderedActionNames = ORDERED_VALID_DELETE_ACTIONS;
            break;
        default:
            throw new IllegalArgumentException("lifecycle type [" + TYPE + "] does not support phase [" + phase.getName() + "]");
        }

        int index = orderedActionNames.indexOf(currentActionName);
        if (index < 0) {
            throw new IllegalArgumentException("[" + currentActionName + "] is not a valid action for phase [" + phase.getName()
                    + "] in lifecycle type [" + TYPE + "]");
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
                    throw new IllegalArgumentException("invalid action [" + actionName + "] " +
                        "defined in phase [" + phase.getName() +"]");
                }
            });
        });

        // Check for forcemerge in 'hot' without a rollover action
        if (phases.stream()
            // Is there a hot phase
            .filter(phase -> HOT_PHASE.equals(phase.getName()))
            // That contains the 'forcemerge' action
            .filter(phase -> phase.getActions().containsKey(ForceMergeAction.NAME))
            // But does *not* contain the 'rollover' action?
            .anyMatch(phase -> phase.getActions().containsKey(RolloverAction.NAME) == false)) {
            // If there is, throw an exception
            throw new IllegalArgumentException("the [" + ForceMergeAction.NAME +
                "] action may not be used in the [" + HOT_PHASE +
                "] phase without an accompanying [" + RolloverAction.NAME + "] action");
        }
    }
}
