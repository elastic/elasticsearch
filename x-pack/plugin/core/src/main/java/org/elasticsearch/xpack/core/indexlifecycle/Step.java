/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;

/**
 * A {@link LifecycleAction} which deletes the index.
 */
public abstract class Step {
    private final String name;
    private final String action;
    private final String phase;
    private final String index;

    public Step(String name, String action, String phase, String index) {
        this.name = name;
        this.action = action;
        this.phase = phase;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public String getAction() {
        return action;
    }

    public String getPhase() {
        return phase;
    }

    public String getIndex() {
        return index;
    }

    public abstract StepResult execute(ClusterState currentState);
}
