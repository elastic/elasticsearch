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

public class ClusterStateUpdateStep extends Step {
    private final Function<ClusterState, ClusterState> updateTask;

    public ClusterStateUpdateStep(String name, String index, String phase, String action, StepKey nextStepKey, Function<ClusterState, ClusterState> updateTask) {
        super(name, action, phase, nextStepKey);
        this.updateTask = updateTask;
    }

    public StepResult execute(ClusterService clusterService, ClusterState currentState, Index index, Client client, LongSupplier nowSupplier) {
        ClusterState updated = null;
        try {
            updated = updateTask.apply(currentState);
            updated = updateStateWithNextStep(updated, nowSupplier, index);
            return new StepResult("done!", null, updated, true, true);
        } catch (Exception e) {
            return new StepResult("something went wrong", e, updated, true, true);
        }
    }
}
