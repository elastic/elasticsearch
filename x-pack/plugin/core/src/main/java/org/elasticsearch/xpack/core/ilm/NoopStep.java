/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.Index;

/**
 * This is a Noop step that can be used for backwards compatibility when removing some step in newer versions.
 * It literally does nothing so that we can safely proceed to the nextStepKey without getting stuck.
 */
public class NoopStep extends ClusterStateWaitStep {
    public static final String NAME = "noop";

    public NoopStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        // As this is a noop step and we don't want to get stuck in, we want it to be retryable
        return true;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        // We always want to move forward with this step so this should always be true
        return new Result(true, null);
    }
}
