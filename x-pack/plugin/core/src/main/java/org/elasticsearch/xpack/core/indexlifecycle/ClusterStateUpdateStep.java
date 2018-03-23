/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.StepResult;

import java.util.function.Function;

public class ClusterStateUpdateStep extends Step {
    private final String name;
    private final String index;
    private final String phase;
    private final String action;
    private final Function<ClusterState, ClusterState> updateTask;

    public ClusterStateUpdateStep(String name, String index, String phase, String action, Function<ClusterState, ClusterState> updateTask) {
        super(name, action, phase, index);
        this.name = name;
        this.index = index;
        this.phase = phase;
        this.action = action;
        this.updateTask = updateTask;
    }

    public StepResult execute(ClusterState clusterState) {
        ClusterState updated = null;
        try {
            updated = updateTask.apply(clusterState);
            return new StepResult("done!", null, updated, true, true);
        } catch (Exception e) {
            return new StepResult("something went wrong", e, updated, true, true);
        }
    }
}
