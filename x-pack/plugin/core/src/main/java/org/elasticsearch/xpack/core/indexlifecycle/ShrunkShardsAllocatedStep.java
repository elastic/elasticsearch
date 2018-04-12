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

public class ShrunkShardsAllocatedStep extends ClusterStateWaitStep {
    public static final String NAME = "enough-shards-allocated";
    private final int numberOfShards;
    private String shrunkIndexPrefix;

    public ShrunkShardsAllocatedStep(StepKey key, StepKey nextStepKey, int numberOfShards, String shrunkIndexPrefix) {
        super(key, nextStepKey);
        this.numberOfShards = numberOfShards;
        this.shrunkIndexPrefix = shrunkIndexPrefix;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    String getShrunkIndexPrefix() {
        return shrunkIndexPrefix;
    }

    @Override
    public boolean isConditionMet(Index index, ClusterState clusterState) {
        // We only want to make progress if all shards of the shrunk index are
        // active
        return clusterState.metaData().index(shrunkIndexPrefix + index.getName()) != null
                && clusterState.metaData().index(shrunkIndexPrefix + index.getName()).getNumberOfShards() == numberOfShards
                && ActiveShardCount.ALL.enoughShardsActive(clusterState, shrunkIndexPrefix + index.getName());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numberOfShards, shrunkIndexPrefix);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ShrunkShardsAllocatedStep other = (ShrunkShardsAllocatedStep) obj;
        return super.equals(obj) &&
                Objects.equals(numberOfShards, other.numberOfShards) &&
                Objects.equals(shrunkIndexPrefix, other.shrunkIndexPrefix);
    }
}
