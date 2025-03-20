/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.index.Index;

import java.util.List;
import java.util.function.Predicate;

final class IndexAllocation {
    private IndexAllocation() {
        // no instances intended
    }

    static boolean isAnyAssignedToNode(ClusterState state, List<Index> indices, Predicate<DiscoveryNode> nodePredicate) {
        for (Index index : indices) {
            IndexMetadata metadata = state.getMetadata().getProject().index(index);
            if (metadata == null) {
                continue;
            }
            IndexRoutingTable routingTable = state.routingTable().index(index);
            if (routingTable == null) {
                continue;
            }
            for (ShardRouting shardRouting : routingTable.randomAllActiveShardsIt()) {
                if (shardRouting.assignedToNode() == false) {
                    continue;
                }
                DiscoveryNode assignedNode = state.nodes().get(shardRouting.currentNodeId());
                if (nodePredicate.test(assignedNode)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Determines whether any of the provided indices is allocated to the warm or cold tier. Machines on these
     * tiers usually use spinning disks.
     *
     * @param state Current cluster state.
     * @param indices A list of indices to check.
     * @return <code>true</code> iff at least one index is allocated to either a warm or cold data node.
     */
    static boolean isAnyOnWarmOrColdTier(ClusterState state, List<Index> indices) {
        return isAnyAssignedToNode(
            state,
            indices,
            // a content node is never considered a warm or cold node
            n -> DataTier.isContentNode(n) == false && (DataTier.isWarmNode(n) || DataTier.isColdNode(n))
        );
    }
}
