/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.Optional;
import java.util.Set;

public class DefaultAllocationDecider
    implements
        AllocationDecider.ShardToNode,
        AllocationDecider.ShardToCluster,
        AllocationDecider.IndexToNode,
        AllocationDecider.ShardRemain,
        AllocationDecider.ShardRebalance,
        AllocationDecider.ClusterRebalance,
        AllocationDecider.ForceDuringReplace,
        AllocationDecider.ForcedInitialShardAllocation,
        AllocationDecider.AutoExpandToNode {
    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    @Override
    public Decision canRebalance(RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    @Override
    public Optional<Set<String>> getForcedInitialShardAllocationToNodes(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Optional.empty();
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }
}
