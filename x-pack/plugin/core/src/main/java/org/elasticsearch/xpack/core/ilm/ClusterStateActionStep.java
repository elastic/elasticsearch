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
 * Updates the cluster state, similar to {@link org.elasticsearch.cluster.ClusterStateUpdateTask}.
 */
public abstract class ClusterStateActionStep extends Step {

    public ClusterStateActionStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    public abstract ClusterState performAction(Index index, ClusterState clusterState);
}
