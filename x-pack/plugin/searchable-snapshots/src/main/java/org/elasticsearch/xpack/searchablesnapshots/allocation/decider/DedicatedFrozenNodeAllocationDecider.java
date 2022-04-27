/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE;

public class DedicatedFrozenNodeAllocationDecider extends AllocationDecider {

    private static final String NAME = "dedicated_frozen_node";

    private static final Decision YES_NOT_DEDICATED_FROZEN_NODE = Decision.single(
        Decision.Type.YES,
        NAME,
        "this node's data roles are not exactly [" + DATA_FROZEN_NODE_ROLE.roleName() + "] so it is not a dedicated frozen node"
    );

    private static final Decision YES_IS_PARTIAL_SEARCHABLE_SNAPSHOT = Decision.single(
        Decision.Type.YES,
        NAME,
        "this index is a frozen searchable snapshot so it can be assigned to this dedicated frozen node"
    );

    private static final Decision NO = Decision.single(
        Decision.Type.NO,
        NAME,
        "this node's data roles are exactly ["
            + DATA_FROZEN_NODE_ROLE.roleName()
            + "] so it may only hold shards from frozen searchable snapshots, but this index is not a frozen searchable snapshot"
    );

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocateToNode(allocation.metadata().getIndexSafe(shardRouting.index()), node.node());
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocateToNode(allocation.metadata().getIndexSafe(shardRouting.index()), node.node());
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return canAllocateToNode(indexMetadata, node.node());
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return canAllocateToNode(indexMetadata, node);
    }

    private static Decision canAllocateToNode(IndexMetadata indexMetadata, DiscoveryNode discoveryNode) {

        boolean hasDataFrozenRole = false;
        boolean hasOtherDataRole = false;
        for (DiscoveryNodeRole role : discoveryNode.getRoles()) {
            if (DATA_FROZEN_NODE_ROLE.equals(role)) {
                hasDataFrozenRole = true;
            } else if (role.canContainData()) {
                hasOtherDataRole = true;
                break;
            }
        }

        if (hasDataFrozenRole == false || hasOtherDataRole) {
            return YES_NOT_DEDICATED_FROZEN_NODE;
        }

        if (indexMetadata.isPartialSearchableSnapshot()) {
            return YES_IS_PARTIAL_SEARCHABLE_SNAPSHOT;
        }

        return NO;
    }
}
