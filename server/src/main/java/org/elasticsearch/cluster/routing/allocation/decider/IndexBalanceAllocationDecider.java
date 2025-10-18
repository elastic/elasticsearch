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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.IndexBalanceConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;

import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * For an index of n shards hosted by a cluster of m nodes, a node should not host
 * significantly more than n / m shards. This allocation decider enforces this principle.
 * This allocation decider excludes any nodes flagged for shutdown from consideration
 * when computing optimal shard distributions.
 */
public class IndexBalanceAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(IndexBalanceAllocationDecider.class);

    public static final String NAME = "index_balance";

    private final IndexBalanceConstraintSettings indexBalanceConstraintSettings;

    public IndexBalanceAllocationDecider(ClusterSettings clusterSettings) {
        this.indexBalanceConstraintSettings = new IndexBalanceConstraintSettings(clusterSettings);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (indexBalanceConstraintSettings.isDeciderEnabled()) {
            Index index = shardRouting.index();
            if (node.hasIndex(index)) {
                var dataNodes = allocation.nodes()
                    .stream()
                    .filter(DiscoveryNode::canContainData)
                    .map(DiscoveryNode::getId)
                    .collect(Collectors.toSet());
                var nodesShuttingDown = allocation.metadata()
                    .nodeShutdowns()
                    .getAll()
                    .values()
                    .stream()
                    .map(SingleNodeShutdownMetadata::getNodeId)
                    .collect(Collectors.toSet());
                var availableDataNodes = dataNodes.stream().filter(Predicate.not(nodesShuttingDown::contains)).collect(Collectors.toSet());

                assert availableDataNodes.isEmpty() == false;
                assert allocation.getClusterState().routingTable(ProjectId.DEFAULT).hasIndex(index);

                var totalShards = allocation.getClusterState().routingTable(ProjectId.DEFAULT).index(index).size();
                var idealAllocation = Math.ceil((double) totalShards / availableDataNodes.size());
                var threshold = (int) Math.ceil(idealAllocation * indexBalanceConstraintSettings.getLoadSkewTolerance());
                var currentAllocation = node.numberOfOwningShardsForIndex(index);

                if (currentAllocation >= threshold) {

                    String rationale =
                        """
                            For index [%s] with [%d] shards, Node [%s] is expected to hold [%.0f] shards for index [%s], based on the total of [%d]
                            nodes available. The configured load skew tolerance is [%.2f], which yields an allocation threshold of
                            Math.ceil([%.0f] × [%.2f]) = [%d] shards. Currently, node [%s] is assigned [%d] shards of index [%s]. Therefore,
                            assigning additional shards is not preferred.
                            """;

                    String explanation = Strings.format(
                        rationale,
                        index,
                        totalShards,
                        node.nodeId(),
                        idealAllocation,
                        index,
                        availableDataNodes.size(),
                        indexBalanceConstraintSettings.getLoadSkewTolerance(),
                        idealAllocation,
                        indexBalanceConstraintSettings.getLoadSkewTolerance(),
                        threshold,
                        node.nodeId(),
                        currentAllocation,
                        index
                    );

                    if (logger.isTraceEnabled()) {
                        logger.trace(explanation);
                    }

                    return allocation.decision(Decision.NOT_PREFERRED, NAME, explanation);
                }
            }

            return Decision.YES;
        } else {
            return Decision.single(Decision.Type.YES, NAME, "Decider is disabled");
        }

    }

}
