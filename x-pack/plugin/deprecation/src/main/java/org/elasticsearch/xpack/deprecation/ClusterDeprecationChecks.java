/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.Locale;

public class ClusterDeprecationChecks {
    /**
     * Upgrading can require the addition of one or more small indices. This method checks that based on configuration we have the room
     * to add a small number of additional shards to the cluster. The goal is to prevent a failure during upgrade.
     * @param clusterState The cluster state, used to get settings and information about nodes
     * @return A deprecation issue if there is not enough room in this cluster to add a few more shards, or null otherwise
     */
    static DeprecationIssue checkShards(ClusterState clusterState) {
        // Make sure we have room to add a small non-frozen index if needed
        final int shardsInFutureNewSmallIndex = 5;
        final int replicasForFutureIndex = 1;
        ShardLimitValidator.Result shardLimitsResult = ShardLimitValidator.checkShardLimitForNormalNodes(
            shardsInFutureNewSmallIndex,
            replicasForFutureIndex,
            clusterState
        );
        if (shardLimitsResult.canAddShards()) {
            return null;
        } else {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "The cluster has too many shards to be able to upgrade",
                "https://ela.st/es-deprecation-8-shard-limit",
                String.format(
                    Locale.ROOT,
                    "Upgrading requires adding a small number of new shards. There is not enough room for %d more "
                        + "shards. Increase the cluster.max_shards_per_node setting, or remove indices "
                        + "to clear up resources.",
                    shardLimitsResult.totalShardsToAdd()
                ),
                false,
                null
            );
        }
    }
}
