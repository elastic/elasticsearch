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
 * Signals that an error was encountered during the execution of a policy on an index.
 */
public class NoopStep extends ClusterStateWaitStep {
    public static final String NAME = "NOOP";

    public NoopStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        // this is noop step we don't want to get stuck in, so we want it to be retryable
        return true;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        // we always want to move forward with this step so this should always be true
        return new Result(true, null);
    }
}
