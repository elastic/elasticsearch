/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Represents the lifecycle of an index from creation to deletion. A
 * {@link TimeseriesLifecyclePolicy} is made up of a set of {@link Phase}s which it will
 * move through. Soon we will constrain the phases using some kinda of lifecycle
 * type which will allow only particular {@link Phase}s to be defined, will
 * dictate the order in which the {@link Phase}s are executed and will define
 * which {@link LifecycleAction}s are allowed in each phase.
 */
public class TimeseriesLifecyclePolicy extends LifecyclePolicy {
    public static final String TYPE = "timeseries";
    static final List<String> VALID_PHASES = Arrays.asList("hot", "warm", "cold", "delete");
    static final Set<String> VALID_HOT_ACTIONS = Sets.newHashSet(RolloverAction.NAME);
    static final Set<String> VALID_WARM_ACTIONS = Sets.newHashSet(AllocateAction.NAME, ReplicasAction.NAME,
        ShrinkAction.NAME, ForceMergeAction.NAME);
    static final Set<String> VALID_COLD_ACTIONS = Sets.newHashSet(AllocateAction.NAME, ReplicasAction.NAME);
    static final Set<String> VALID_DELETE_ACTIONS = Sets.newHashSet(DeleteAction.NAME);

    /**
     * @param name
     *            the name of this {@link TimeseriesLifecyclePolicy}
     * @param phases
     *            a {@link Map} of {@link Phase}s which make up this
     *            {@link TimeseriesLifecyclePolicy}.
     */
    public TimeseriesLifecyclePolicy(String name, Map<String, Phase> phases) {
        super(name, phases);
    }

    /**
     * For Serialization
     */
    public TimeseriesLifecyclePolicy(StreamInput in) throws IOException {
        super(in);
    }

    public static TimeseriesLifecyclePolicy parse(XContentParser parser, Object context) {
        ToXContentContext factory = (ToXContentContext) context;
        return new TimeseriesLifecyclePolicy(factory.getName(), factory.getPhases());
    }

    @Override
    protected String getType() {
        return TYPE;
    }

    @Override
    protected Phase getFirstPhase() {
        Phase firstPhase = phases.get("hot");
        if (firstPhase == null) {
            firstPhase = phases.get("warm");
        }
        if (firstPhase == null) {
            firstPhase = phases.get("cold");
        }
        if (firstPhase == null) {
            firstPhase = phases.get("delete");
        }
        return firstPhase;
    }

    @Override
    protected Phase nextPhase(@Nullable Phase currentPhase) {
        if (currentPhase == null) {
            return getFirstPhase();
        }

        // VALID_PHASES is in order of execution
        boolean readyToSetNext = false;
        for (String phaseName : VALID_PHASES) {
            if (readyToSetNext && phases.containsKey(phaseName)) {
                return phases.get(phaseName);
            }
            if (phaseName.equals(currentPhase.getName())) {
                readyToSetNext = true;
            }
        }

        return null;
    }

    /**
     * This action provider returns an ordering for the actions within each of the four timeseries phases.
     *  Hot Phase:
     *      The Hot Phase only supports the {@link RolloverAction} and so that is the only action to order
     *  Warm Phase:
     *      The Warm Phase executes the supported actions in a slightly complicated order for the sake of
     *      optimization. Assuming the {@link ReplicasAction} is specified, it will run first or last depending
     *      on whether it increases, decreases, or keeps the existing replica count. If number-of-replicas is
     *      kept the same, or reduced, then {@link ReplicasAction} is executed first, otherwise, it is last.
     *      So the ordering looks something like this:
     *          - {@link ReplicasAction} (if action.number_of_replicas lte idxMeta.number_of_replicas)
     *          - {@link AllocateAction}
     *          - {@link ShrinkAction}
     *          - {@link ForceMergeAction}
     *          - {@link ReplicasAction} (if action.number_of_replicas gt idxMeta.number_of_replicas)
     *
     *      NOCOMMIT: there may exist further optimizations to this when {@link ShrinkAction} is specified.
     *
     * @param context the index lifecycle context for this phase at the time of execution
     * @param phase the current phase for which to provide an action provider
     * @return the {@link org.elasticsearch.xpack.indexlifecycle.LifecyclePolicy.NextActionProvider} for {@code phase}.
     */
    @Override
    protected NextActionProvider getActionProvider(IndexLifecycleContext context, Phase phase) {
        Map<String, LifecycleAction> actions = phase.getActions();
        switch (phase.getName()) {
            case "hot":
                // The hot-phase only has one action, either start with it, or return null. Simple as that!
                return (action) -> (action == null) ? actions.getOrDefault(RolloverAction.NAME, null) : null;
            case "warm":
                return (action) -> {
                    ReplicasAction replicasAction = (ReplicasAction) actions.getOrDefault(ReplicasAction.NAME, null);
                    if (action == null) {
                        if (replicasAction != null && replicasAction.getNumberOfReplicas() <= context.getNumberOfReplicas()) {
                            return replicasAction;
                        }
                        return Stream.of(AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME, ReplicasAction.NAME)
                            .map(a -> actions.getOrDefault(a, null))
                            .filter(Objects::nonNull).findFirst().orElse(null);
                    } else if (action instanceof ReplicasAction) {
                        return Stream.of(AllocateAction.NAME, ShrinkAction.NAME, ForceMergeAction.NAME)
                            .map(a -> actions.getOrDefault(a, null))
                            .filter(Objects::nonNull).findFirst().orElse(null);
                    } else if (action instanceof AllocateAction) {
                        return Stream.of(ShrinkAction.NAME, ForceMergeAction.NAME, ReplicasAction.NAME)
                            .map(a -> actions.getOrDefault(a, null))
                            .filter(Objects::nonNull).findFirst().orElse(null);
                    } else if (action instanceof ShrinkAction) {
                        return Stream.of(ForceMergeAction.NAME, ReplicasAction.NAME)
                            .map(a -> actions.getOrDefault(a, null))
                            .filter(Objects::nonNull).findFirst().orElse(null);
                    } else if (action instanceof ForceMergeAction) {
                        return replicasAction;
                    }
                    return null;
                };
            case "cold":
                return (action) -> {
                    ReplicasAction replicasAction = (ReplicasAction) actions.getOrDefault(ReplicasAction.NAME, null);
                    LifecycleAction allocateAction = actions.getOrDefault(AllocateAction.NAME, null);
                    if (action == null) {
                        if (replicasAction != null && replicasAction.getNumberOfReplicas() <= context.getNumberOfReplicas()) {
                            return replicasAction;
                        } else if (allocateAction != null) {
                            return allocateAction;
                        }
                        return replicasAction;
                    } else if (action instanceof ReplicasAction) {
                        return allocateAction;
                    } else if (action instanceof AllocateAction) {
                        return replicasAction;
                    }
                    return null;
                };
            case "delete":
                return (action) -> {
                    if (action == null) {
                        return actions.getOrDefault(DeleteAction.NAME, null);
                    }
                    return null;
                };
            default:
                throw new IllegalArgumentException("phase [" + phase.getName() + "] is invalid for policy [timeseries]");
        }
    }

    @Override
    public void validate(Collection<Phase> phases) {
        Set<String> allowedPhases = new HashSet<>(VALID_PHASES);
        Map<String, Set<String>> allowedActions = new HashMap<>(allowedPhases.size());
        allowedActions.put("hot", VALID_HOT_ACTIONS);
        allowedActions.put("warm", VALID_WARM_ACTIONS);
        allowedActions.put("cold", VALID_COLD_ACTIONS);
        allowedActions.put("delete", VALID_DELETE_ACTIONS);
        phases.forEach(phase -> {
            if (allowedPhases.contains(phase.getName()) == false) {
                throw new IllegalArgumentException("Timeseries lifecycle does not support phase [" + phase.getName() + "]");
            }
            phase.getActions().forEach((actionName, action) -> {
                if (allowedActions.get(phase.getName()).contains(actionName) == false) {
                    throw new IllegalArgumentException("invalid action [" + actionName + "] " +
                        "defined in phase [" + phase.getName() +"]");
                }
            });
        });
    }
}
