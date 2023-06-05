/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

public class NodeReplacementAllocationDecider extends AllocationDecider {

    public static final String NAME = "node_replacement";

    static final Decision YES__RECONCILING = Decision.single(Decision.Type.YES, NAME, "this decider is ignored during reconciliation");

    static final Decision YES__NO_REPLACEMENTS = Decision.single(Decision.Type.YES, NAME, "there are no ongoing node replacements");

    static final Decision YES__NO_APPLICABLE_REPLACEMENTS = Decision.single(
        Decision.Type.YES,
        NAME,
        "none of the ongoing node replacements relate to the allocation of this shard"
    );

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation) == false) {
            return YES__NO_REPLACEMENTS;
        } else if (replacementFromSourceToTarget(allocation, shardRouting.currentNodeId(), node.node().getName())) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "node [%s] is replacing node [%s], and may receive shards from it",
                node.nodeId(),
                shardRouting.currentNodeId()
            );
        } else if (isReplacementSource(allocation, shardRouting.currentNodeId())) {
            if (allocation.isReconciling()) {
                // We permit moving shards off the source node during reconcilation so that they can go onto their desired nodes even if
                // the desired node is different from the replacement target.
                return YES__RECONCILING;
            }

            return allocation.decision(
                Decision.NO,
                NAME,
                "node [%s] is being replaced, and its shards may only be allocated to the replacement target [%s]",
                shardRouting.currentNodeId(),
                getReplacementName(allocation, shardRouting.currentNodeId())
            );
        } else if (isReplacementSource(allocation, node.nodeId())) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "node [%s] is being replaced by [%s], so no data may be allocated to it",
                node.nodeId(),
                getReplacementName(allocation, node.nodeId()),
                shardRouting.currentNodeId()
            );
        } else if (isReplacementTargetName(allocation, node.node().getName())) {
            if (allocation.isReconciling() && shardRouting.unassigned() == false) {
                // We permit moving _existing_ shards onto the target during reconcilation so that they stay out of the way of other shards
                // moving off the source node. But we don't allow any unassigned shards to be assigned to the target since this could
                // prevent the node from being vacated.
                return YES__RECONCILING;
            }

            final SingleNodeShutdownMetadata shutdown = allocation.replacementTargetShutdowns().get(node.node().getName());
            return allocation.decision(
                Decision.NO,
                NAME,
                "node [%s] is replacing the vacating node [%s], only data currently allocated to the source node "
                    + "may be allocated to it until the replacement is complete",
                node.nodeId(),
                shutdown == null ? null : shutdown.getNodeId(),
                shardRouting.currentNodeId()
            );
        } else {
            return YES__NO_APPLICABLE_REPLACEMENTS;
        }
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation) == false) {
            return YES__NO_REPLACEMENTS;
        } else if (isReplacementSource(allocation, node.nodeId())) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "node [%s] is being replaced by node [%s], so no data may remain on it",
                node.nodeId(),
                getReplacementName(allocation, node.nodeId())
            );
        } else {
            return YES__NO_APPLICABLE_REPLACEMENTS;
        }
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation) == false) {
            return YES__NO_REPLACEMENTS;
        } else if (isReplacementTargetName(allocation, node.getName())) {
            final SingleNodeShutdownMetadata shutdown = allocation.replacementTargetShutdowns().get(node.getName());
            final String sourceNodeId = shutdown != null ? shutdown.getNodeId() : null;
            final boolean hasShardsAllocatedOnSourceOrTarget = hasShardOnNode(indexMetadata, node.getId(), allocation)
                || (sourceNodeId != null && hasShardOnNode(indexMetadata, sourceNodeId, allocation));

            if (hasShardsAllocatedOnSourceOrTarget) {
                return allocation.decision(
                    Decision.YES,
                    NAME,
                    "node [%s] is a node replacement target for node [%s], "
                        + "shard can auto expand to it as it was already present on the source node",
                    node.getId(),
                    sourceNodeId
                );
            } else {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node [%s] is a node replacement target for node [%s], "
                        + "shards cannot auto expand to be on it until the replacement is complete",
                    node.getId(),
                    sourceNodeId
                );
            }
        } else if (isReplacementSource(allocation, node.getId())) {
            final SingleNodeShutdownMetadata shutdown = allocation.getClusterState().metadata().nodeShutdowns().get(node.getId());
            final String replacementNodeName = shutdown != null ? shutdown.getTargetNodeName() : null;
            final boolean hasShardOnSource = hasShardOnNode(indexMetadata, node.getId(), allocation)
                && shutdown != null
                && allocation.getClusterState().getNodes().hasByName(replacementNodeName) == false;

            if (hasShardOnSource) {
                return allocation.decision(
                    Decision.YES,
                    NAME,
                    "node [%s] is being replaced by [%s], shards can auto expand to be on it "
                        + "while replacement node has not joined the cluster",
                    node.getId(),
                    replacementNodeName
                );
            } else {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node [%s] is being replaced by [%s], shards cannot auto expand to be on it",
                    node.getId(),
                    replacementNodeName
                );
            }
        } else {
            return YES__NO_APPLICABLE_REPLACEMENTS;
        }
    }

    private static boolean hasShardOnNode(IndexMetadata indexMetadata, String nodeId, RoutingAllocation allocation) {
        RoutingNode node = allocation.routingNodes().node(nodeId);
        return node != null && node.numberOfOwningShardsForIndex(indexMetadata.getIndex()) >= 1;
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (replacementFromSourceToTarget(allocation, shardRouting.currentNodeId(), node.node().getName())) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "node [%s] is being replaced by node [%s], and can be force vacated to the target",
                shardRouting.currentNodeId(),
                node.nodeId()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "shard is not on the source of a node replacement relocated to the replacement target"
            );
        }
    }

    @Override
    public Decision canAllocateReplicaWhenThereIsRetentionLease(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (isReplacementTargetName(allocation, node.node().getName())) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "node [%s] is a node replacement target and can have a previously allocated replica re-allocated to it",
                node.nodeId()
            );
        } else {
            return canAllocate(shardRouting, node, allocation);
        }
    }

    /**
     * Returns true if there are any node replacements ongoing in the cluster
     */
    private static boolean replacementOngoing(RoutingAllocation allocation) {
        return allocation.replacementTargetShutdowns().isEmpty() == false;
    }

    /**
     * Returns true if there is a replacement currently ongoing from the source to the target node id
     */
    private static boolean replacementFromSourceToTarget(RoutingAllocation allocation, String sourceNodeId, String targetNodeName) {
        if (replacementOngoing(allocation) == false) {
            return false;
        }
        if (sourceNodeId == null || targetNodeName == null) {
            return false;
        }
        var shutdown = allocation.metadata().nodeShutdowns().get(sourceNodeId, SingleNodeShutdownMetadata.Type.REPLACE);
        return shutdown != null && shutdown.getTargetNodeName().equals(targetNodeName);
    }

    /**
     * Returns true if the given node id is the source (the replaced node) of an ongoing node replacement
     */
    private static boolean isReplacementSource(RoutingAllocation allocation, String nodeId) {
        if (nodeId == null || replacementOngoing(allocation) == false) {
            return false;
        }
        return allocation.metadata().nodeShutdowns().contains(nodeId, SingleNodeShutdownMetadata.Type.REPLACE);
    }

    /**
     * Returns true if the given node name (not the id!) is the target (the replacing node) of an ongoing node replacement
     */
    private static boolean isReplacementTargetName(RoutingAllocation allocation, String nodeName) {
        if (nodeName == null || replacementOngoing(allocation) == false) {
            return false;
        }
        return allocation.replacementTargetShutdowns().get(nodeName) != null;
    }

    private static String getReplacementName(RoutingAllocation allocation, String nodeIdBeingReplaced) {
        if (nodeIdBeingReplaced == null || replacementOngoing(allocation) == false) {
            return null;
        }
        var metadata = allocation.metadata().nodeShutdowns().get(nodeIdBeingReplaced, SingleNodeShutdownMetadata.Type.REPLACE);
        return metadata != null ? metadata.getTargetNodeName() : null;
    }
}
