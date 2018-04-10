/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Objects;

public class ReplicasAllocatedStep extends ClusterStateWaitStep {
    public static final String NAME = "enough-shards-allocated";
    private int numberReplicas;

    public ReplicasAllocatedStep(StepKey key, StepKey nextStepKey, int numberReplicas) {
        super(key, nextStepKey);
        this.numberReplicas = numberReplicas;
    }

    int getNumberReplicas() {
        return numberReplicas;
    }

    @Override
    public boolean isConditionMet(Index index, ClusterState clusterState) {
        IndexMetaData idxMeta = clusterState.metaData().index(index);
        if (idxMeta == null) {
            throw new IndexNotFoundException("Index not found when executing " + getKey().getAction() + " lifecycle action.",
                    index.getName());
        }
        // We only want to make progress if the cluster state reflects the number of replicas change and all shards are active
        return idxMeta.getNumberOfReplicas() == numberReplicas && ActiveShardCount.ALL.enoughShardsActive(clusterState, index.getName());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numberReplicas);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ReplicasAllocatedStep other = (ReplicasAllocatedStep) obj;
        return super.equals(obj) &&
                Objects.equals(numberReplicas, other.numberReplicas);
    }
}
