/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.Index;

public class EnoughShardsWaitStep extends ClusterStateWaitStep {
    public EnoughShardsWaitStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isConditionMet(Index index, ClusterState clusterState) {
        // We only want to make progress if all shards are active
        return ActiveShardCount.ALL.enoughShardsActive(clusterState, index.getName());
    }
}
