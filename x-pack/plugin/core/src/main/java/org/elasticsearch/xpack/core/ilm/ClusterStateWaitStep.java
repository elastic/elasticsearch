/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * Checks whether a condition has been met based on the cluster state.
 * <p>
 * If checking a condition not based on the cluster state, or which may take time to evaluate, use {@link AsyncWaitStep}.
 */
public abstract class ClusterStateWaitStep extends Step {

    public ClusterStateWaitStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    public abstract Result isConditionMet(Index index, ProjectState currentState);

    /**
     * Whether the step can be completed at all. This only affects the
     * {@link ClusterStateWaitUntilThresholdStep} which waits for a threshold to be met before
     * retrying. Setting this to false means that ILM should retry the sequence immediately without
     * waiting for the threshold to be met.
     */
    public boolean isCompletable() {
        return true;
    }

    public record Result(boolean complete, ToXContentObject informationContext) {}

}
