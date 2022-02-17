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

    private final Integer numberOfShards;

    private static final Logger logger = LogManager.getLogger(CheckTargetShardsCountStep.class);

    CheckTargetShardsCountStep(StepKey key, StepKey nextStepKey, Integer numberOfShards) {
        super(key, nextStepKey);
        this.numberOfShards = numberOfShards;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    public Integer getNumberOfShards() {
        return numberOfShards;
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
        if (numberOfShards != null) {
            int sourceNumberOfShards = indexMetadata.getNumberOfShards();
            if (sourceNumberOfShards % numberOfShards != 0) {
                String policyName = indexMetadata.getLifecyclePolicyName();
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
}
