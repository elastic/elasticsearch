/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * Checks whether a condition has been met based on the cluster state.
 *
 * If checking a condition not based on the cluster state, or which may take time to evaluate, use {@link AsyncWaitStep}.
 */
public abstract class ClusterStateWaitStep extends Step {

    public ClusterStateWaitStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    public abstract Result isConditionMet(Index index, ClusterState clusterState);

    /**
     * Whether the step can be completed at all. This only affects the
     * {@link ClusterStateWaitUntilThresholdStep} which waits for a threshold to be met before
     * retrying. Setting this to false means that ILM should retry the sequence immediately without
     * waiting for the threshold to be met.
     */
    public boolean isCompletable() {
        return true;
    }

    public static class Result {
        private final boolean complete;
        private final ToXContentObject infomationContext;

        public Result(boolean complete, ToXContentObject infomationContext) {
            this.complete = complete;
            this.infomationContext = infomationContext;
        }

        public boolean isComplete() {
            return complete;
        }

        public ToXContentObject getInfomationContext() {
            return infomationContext;
        }
    }

}
