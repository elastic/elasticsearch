/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.step.info.SingleMessageFieldInfo;

import java.util.Locale;

/**
 * This step checks whether the new shrunken index's shards count is a factor of the source index's shards count.
 */
public class CheckTargetShardsCountStep extends ClusterStateWaitStep {

    public static final String NAME = "check-target-shards-count";

    private static final Logger logger = LogManager.getLogger(CheckTargetShardsCountStep.class);

    CheckTargetShardsCountStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        if (indexMetadata == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            return new Result(false, null);
        }
        String indexName = indexMetadata.getIndex().getName();
        String policyName = indexMetadata.getLifecyclePolicyName();
        Integer numberOfShards = getTargetNumberOfShards(policyName, clusterState);
        if (numberOfShards != null) {
            int sourceNumberOfShards = indexMetadata.getNumberOfShards();
            if (sourceNumberOfShards % numberOfShards != 0) {
                String errorMessage = String.format(
                    Locale.ROOT,
                    "lifecycle action of policy [%s] for index [%s] cannot make progress "
                        + "because the target shards count [%d] must be a factor of the source index's shards count [%d]",
                    policyName,
                    indexName,
                    numberOfShards,
                    sourceNumberOfShards
                );
                logger.debug(errorMessage);
                return new Result(false, new SingleMessageFieldInfo(errorMessage));
            }
        }

        return new Result(true, null);
    }

    private Integer getTargetNumberOfShards(String policyName, ClusterState clusterState) {
        IndexLifecycleMetadata indexLifecycleMetadata = clusterState.metadata().custom(IndexLifecycleMetadata.TYPE);
        LifecycleAction lifecycleAction = indexLifecycleMetadata.getPolicyMetadatas()
            .get(policyName)
            .getPolicy()
            .getPhases()
            .get(this.getKey().getPhase())
            .getActions()
            .get(this.getKey().getAction());
        if (lifecycleAction instanceof WithTargetNumberOfShards withTargetNumberOfShards) {
            return withTargetNumberOfShards.getNumberOfShards();
        } else {
            throw new IllegalStateException(
                "The action [" + getKey().getName() + "] this step is part of should be able to provide a target shard count"
            );
        }
    }
}
