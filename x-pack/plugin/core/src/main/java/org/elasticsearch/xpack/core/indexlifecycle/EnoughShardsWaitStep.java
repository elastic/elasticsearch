/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.Index;

import java.util.Objects;

public class EnoughShardsWaitStep extends ClusterStateWaitStep {
    public static final String NAME = "enough-shards-allocated";
    private final int numberOfShards;

    public EnoughShardsWaitStep(StepKey key, StepKey nextStepKey, int numberOfShards) {
        super(key, nextStepKey);
        this.numberOfShards = numberOfShards;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    @Override
    public boolean isConditionMet(Index index, ClusterState clusterState) {
        // We only want to make progress if all shards are active
        return clusterState.metaData().index(index).getNumberOfShards() == numberOfShards &&
            ActiveShardCount.ALL.enoughShardsActive(clusterState, index.getName());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numberOfShards);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        EnoughShardsWaitStep other = (EnoughShardsWaitStep) obj;
        return super.equals(obj) &&
                Objects.equals(numberOfShards, other.numberOfShards);
    }
}
