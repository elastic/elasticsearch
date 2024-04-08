/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

/**
 * An allocation decider that prevents multiple instances of the same shard to
 * be allocated on the same {@code node}.
 *
 * The {@link #CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING} setting allows to perform a check to prevent
 * allocation of multiple instances of the same shard on a single {@code host},
 * based on host name and host address. Defaults to `false`, meaning that no
 * check is performed by default.
 *
 * <p>
 * Note: this setting only applies if multiple nodes are started on the same
 * {@code host}. Allocations of multiple copies of the same shard on the same
 * {@code node} are not allowed independently of this setting.
 * </p>
 */
public class SameShardAllocationDecider extends AllocationDecider {

    public static final String NAME = "same_shard";

    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.same_shard.host",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile boolean sameHost;

    public SameShardAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING, this::setSameHost);
    }

    /**
     * Sets the same host setting.  {@code true} if allocating the same shard copy to the same host
     * should not be allowed, even when multiple nodes are being run on the same host.  {@code false}
     * otherwise.
     */
    private void setSameHost(boolean sameHost) {
        this.sameHost = sameHost;
    }

    private static final Decision YES_NONE_HOLD_COPY = Decision.single(
        Decision.Type.YES,
        NAME,
        "none of the nodes on this host hold a copy of this shard"
    );

    private static final Decision YES_AUTO_EXPAND_ALL = Decision.single(
        Decision.Type.YES,
        NAME,
        "same-host allocation is ignored, this index is set to auto-expand to all nodes"
    );

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        Iterable<ShardRouting> assignedShards = allocation.routingNodes().assignedShards(shardRouting.shardId());
        Decision decision = decideSameNode(shardRouting, node, allocation, assignedShards);
        if (decision.type() == Decision.Type.NO || sameHost == false) {
            // if its already a NO decision looking at the node, or we aren't configured to look at the host, return the decision
            return decision;
        }
        if (allocation.metadata().getIndexSafe(shardRouting.index()).getAutoExpandReplicas().expandToAllNodes()) {
            return YES_AUTO_EXPAND_ALL;
        }
        if (node.node() != null) {
            assert Strings.hasLength(node.node().getHostAddress()) : node;
            for (ShardRouting assignedShard : assignedShards) {
                DiscoveryNode checkNode = allocation.nodes().get(assignedShard.currentNodeId());
                assert checkNode != null;
                // check if its on the same host as the one we want to allocate to
                assert Strings.hasLength(checkNode.getHostAddress()) : checkNode;
                if (checkNode.getHostAddress().equals(node.node().getHostAddress())) {
                    return allocation.debugDecision() ? debugNoAlreadyAllocatedToHost(node, checkNode, allocation) : Decision.NO;
                }
            }
        }
        return YES_NONE_HOLD_COPY;
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node, allocation);
    }

    private static Decision debugNoAlreadyAllocatedToHost(RoutingNode newNode, DiscoveryNode existingNode, RoutingAllocation allocation) {
        return allocation.decision(
            Decision.NO,
            NAME,
            """
                cannot allocate to node [%s] because a copy of this shard is already allocated to node [%s] with the same host \
                address [%s] and [%s] is [true] which forbids more than one node on each host from holding a copy of this shard""",
            newNode.nodeId(),
            existingNode.getId(),
            newNode.node().getHostAddress(),
            CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.getKey()
        );
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call force allocate on a non-primary shard";
        Iterable<ShardRouting> assignedShards = allocation.routingNodes().assignedShards(shardRouting.shardId());
        return decideSameNode(shardRouting, node, allocation, assignedShards);
    }

    private static final Decision YES_NO_COPY = Decision.single(Decision.Type.YES, NAME, "this node does not hold a copy of this shard");

    private static Decision decideSameNode(
        ShardRouting shardRouting,
        RoutingNode node,
        RoutingAllocation allocation,
        Iterable<ShardRouting> assignedShards
    ) {
        boolean debug = allocation.debugDecision();
        for (ShardRouting assignedShard : assignedShards) {
            if (node.nodeId().equals(assignedShard.currentNodeId())) {
                return debug ? debugNo(shardRouting, assignedShard) : Decision.NO;
            }
        }
        return YES_NO_COPY;
    }

    private static Decision debugNo(ShardRouting shardRouting, ShardRouting assignedShard) {
        final String explanation;
        if (assignedShard.isSameAllocation(shardRouting)) {
            explanation = "this shard is already allocated to this node [" + shardRouting.toString() + "]";
        } else {
            explanation = "a copy of this shard is already allocated to this node [" + assignedShard + "]";
        }
        return Decision.single(Decision.Type.NO, NAME, explanation);
    }
}
