/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

public class ClusterDeprecationChecks {

    static DeprecationIssue checkShardLimit(ClusterState state) {
        int shardsPerNode = MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(state.metaData().settings());
        int nodeCount = state.getNodes().getDataNodes().size();
        int maxShardsInCluster = shardsPerNode * nodeCount;
        int currentOpenShards = state.getMetaData().getTotalOpenIndexShards();

        if (currentOpenShards >= maxShardsInCluster) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Number of open shards exceeds cluster soft limit",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html",
                "There are [" + currentOpenShards + "] open shards in this cluster, but the cluster is limited to [" +
                    shardsPerNode + "] per data node, for [" + maxShardsInCluster + "] maximum.");
        }
        return null;
    }
}
