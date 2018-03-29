/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.Index;

public abstract class ClusterStateWaitStep extends Step {

    public ClusterStateWaitStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    public abstract boolean isConditionMet(Index index, ClusterState clusterState);

}
