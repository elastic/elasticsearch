/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import static org.elasticsearch.discovery.DiscoverySettings.NO_MASTER_BLOCK_SETTING;

public class ClusterDeprecationChecks {

    static DeprecationIssue checkShardLimit(ClusterState state) {
        int shardsPerNode = MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(state.metaData().settings());
        int nodeCount = state.getNodes().getDataNodes().size();
        int maxShardsInCluster = shardsPerNode * nodeCount;
        int currentOpenShards = state.getMetaData().getTotalOpenIndexShards();

        if (nodeCount > 0 && currentOpenShards >= maxShardsInCluster) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Number of open shards exceeds cluster soft limit",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_cluster_wide_shard_soft_limit",
                "There are [" + currentOpenShards + "] open shards in this cluster, but the cluster is limited to [" +
                    shardsPerNode + "] per data node, for [" + maxShardsInCluster + "] maximum.");
        }
        return null;
    }

    static DeprecationIssue checkNoMasterBlock(ClusterState state) {
        if (state.metaData().settings().hasValue(NO_MASTER_BLOCK_SETTING.getKey())) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Master block setting renamed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "_new_name_for_literal_no_maaster_block_literal_setting",
                "The settings discovery.zen.no_master_block has been renamed to cluster.no_master_block");
        }
        return null;
    }

    static DeprecationIssue checkClusterName(ClusterState state) {
        String clusterName = state.getClusterName().value();
        if (clusterName.contains(":")) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Cluster name cannot contain ':'",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_literal_literal_is_no_longer_allowed_in_cluster_name",
                "This cluster is named [" + clusterName + "], which contains the illegal character ':'.");
        }
        return null;
    }
}
