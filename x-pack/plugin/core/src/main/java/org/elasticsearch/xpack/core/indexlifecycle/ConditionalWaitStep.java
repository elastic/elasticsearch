/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.StepResult;

import java.util.function.Function;
import java.util.function.LongSupplier;

public class ConditionalWaitStep extends Step {
    private final Function<ClusterState, Boolean> condition;

    public ConditionalWaitStep(String name, String phase, String action, StepKey nextStepKey, Function<ClusterState, Boolean> condition) {
        super(name, action, phase, nextStepKey);
        this.condition = condition;
    }

    @Override
    public StepResult execute(ClusterService clusterService, ClusterState currentState, Index index, Client client, LongSupplier nowSupplier) {
        boolean isComplete = condition.apply(currentState);
        return new StepResult(String.valueOf(isComplete), null, currentState, true, isComplete);
    }
}
