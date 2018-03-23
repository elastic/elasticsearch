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

public class ConditionalWaitStep extends Step {
    private final Function<ClusterState, Boolean> condition;

    public ConditionalWaitStep(String name, String index, String phase, String action, Function<ClusterState, Boolean> condition) {
        super(name, action, phase, index);
        this.condition = condition;
    }

    @Override
    public StepResult execute(ClusterState currentState) {
        boolean isComplete = condition.apply(currentState);
        return new StepResult(String.valueOf(isComplete), null, currentState, true, isComplete);
    }
}
