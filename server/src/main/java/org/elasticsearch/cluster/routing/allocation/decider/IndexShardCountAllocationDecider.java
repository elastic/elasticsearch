/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.IndexShardCountConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;

/**
 * For an index of n shards hosted by a cluster of m nodes, a node should not host
 * significantly more than n / m shards. This allocation decider enforces this principle.
 * This allocation decider excludes any nodes flagged for shutdown from consideration
 * when computing optimal shard distributions.
 */
public class IndexShardCountAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(IndexShardCountAllocationDecider.class);

    public static final String NAME = "index_shard_count";

    private final IndexShardCountConstraintSettings indexShardCountConstraintSettings;

    public IndexShardCountAllocationDecider(ClusterSettings clusterSettings) {
        this.indexShardCountConstraintSettings = new IndexShardCountConstraintSettings(clusterSettings);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (indexShardCountConstraintSettings.getIndexShardCountDeciderStatus().disabled()) {
            return Decision.single(Decision.Type.YES, NAME, "Decider is disabled");
        }

        Index index = shardRouting.index();
        if (node.hasIndex(index)) {
            var nodeShutdowns = allocation.metadata().nodeShutdowns().getAll().size();
            var allNodes = allocation.nodes().size();
            var availableNodes = allNodes - nodeShutdowns;

            assert availableNodes > 0;
            assert allocation.getClusterState().routingTable(ProjectId.DEFAULT).hasIndex(index);

            var totalShards = allocation.getClusterState().routingTable(ProjectId.DEFAULT).index(index).size();
            var idealAllocation = Math.ceil((double) totalShards / availableNodes);
            var threshold =  (int) Math.ceil(idealAllocation * indexShardCountConstraintSettings.getLoadSkewTolerance());
            var currentAllocation = node.numberOfOwningShardsForIndex(index);

            if (currentAllocation >= threshold) {

                String rationale = """
                    For index [%s] with [%d] shards, Node [%s] is expected to hold [%.0f] shards for index [%s], based on the total of [%d]
                    nodes available. The configured load skew tolerance is [%.2f], which yields an allocation threshold of
                    Math.ceil([%0.f] × [%.2f]) = [%d] shards. Currently, node [%s] is assigned [%d] shards of index [%s]. Therefore,
                    assigning additional shards is not preferred.
                    """;

                String explanation = Strings.format(rationale,
                    index, totalShards, node.nodeId(), idealAllocation, index, availableNodes,
                    indexShardCountConstraintSettings.getLoadSkewTolerance(), idealAllocation,
                    indexShardCountConstraintSettings.getLoadSkewTolerance(), threshold, node.nodeId(), currentAllocation, index);

                if (logger.isTraceEnabled()) {
                    logger.trace(explanation);
                }

                return allocation.decision(Decision.NOT_PREFERRED, NAME, explanation);
            }
        }

        return Decision.YES;
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (indexShardCountConstraintSettings.getIndexShardCountDeciderStatus().disabled()) {
            return Decision.single(Decision.Type.YES, NAME, "Decider is disabled");
        }

        // TODO: implement
        return Decision.single(Decision.Type.YES, NAME, "canRemain() is not yet implemented");
    }






}
