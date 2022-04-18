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

public class ClusterDeprecationChecks {
    static DeprecationIssue checkShards(ClusterState clusterState) {
        // Make sure we have room to add a small non-frozen index if needed
        if (ShardLimitValidator.canAddShardsToCluster(5, 1, clusterState, false)) {
            return null;
        } else {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "The cluster has too many shards to be able to upgrade",
                "https://ela.st/es-deprecation-7-shard-limit",
                "Delete indices to clear up space",
                false,
                null
            );
        }
    }
}
