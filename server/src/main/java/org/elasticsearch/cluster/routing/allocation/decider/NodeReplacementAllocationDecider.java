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

import java.util.Map;
import java.util.Optional;

public class NodeReplacementAllocationDecider extends AllocationDecider {

    public static final String NAME = "node_replacement";

    static final Decision NO_REPLACEMENTS = Decision.single(
        Decision.Type.YES,
        NAME,
        "neither the source nor target node are part of an ongoing node replacement (no replacements)"
    );

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation) == false) {
            return NO_REPLACEMENTS;
        } else if (replacementFromSourceToTarget(allocation, shardRouting.currentNodeId(), node.node().getName())) {
            return Decision.single(
                Decision.Type.YES,
                NAME,
                "node [%s] is replacing node [%s], and may receive shards from it",
                shardRouting.currentNodeId(),
                node.nodeId()
            );
        } else if (isReplacementSource(allocation, shardRouting.currentNodeId())) {
            return Decision.single(
                Decision.Type.NO,
                NAME,
                "node [%s] is being replaced, and its shards may only be allocated to the replacement target [%s]",
                shardRouting.currentNodeId(),
                getReplacementName(allocation, shardRouting.currentNodeId())
            );
        } else if (isReplacementSource(allocation, node.nodeId())) {
            return Decision.single(
                Decision.Type.NO,
                NAME,
                "node [%s] is being replaced by [%s], so no data may be allocated to it",
                node.nodeId(),
                getReplacementName(allocation, node.nodeId()),
                shardRouting.currentNodeId()
            );
        } else if (isReplacementTargetName(allocation, node.node().getName())) {
            final SingleNodeShutdownMetadata shutdown = allocation.replacementTargetShutdowns().get(node.node().getName());
            return Decision.single(
                Decision.Type.NO,
                NAME,
                "node [%s] is replacing the vacating node [%s], only data currently allocated to the source node "
                    + "may be allocated to it until the replacement is complete",
                node.nodeId(),
                shutdown == null ? null : shutdown.getNodeId(),
                shardRouting.currentNodeId()
            );
        } else {
            return Decision.single(Decision.Type.YES, NAME, "neither the source nor target node are part of an ongoing node replacement");
        }
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation) == false) {
            return NO_REPLACEMENTS;
        } else if (isReplacementSource(allocation, node.nodeId())) {
            return Decision.single(
                Decision.Type.NO,
                NAME,
                "node [%s] is being replaced by node [%s], so no data may remain on it",
                node.nodeId(),
                getReplacementName(allocation, node.nodeId())
            );
        } else {
            return Decision.single(Decision.Type.YES, NAME, "node [%s] is not being replaced", node.nodeId());
        }
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation) == false) {
            return NO_REPLACEMENTS;
        } else if (isReplacementTargetName(allocation, node.getName())) {
            final SingleNodeShutdownMetadata shutdown = allocation.replacementTargetShutdowns().get(node.getName());
            return Decision.single(
                Decision.Type.NO,
                NAME,
                "node [%s] is a node replacement target for node [%s], "
                    + "shards cannot auto expand to be on it until the replacement is complete",
                node.getId(),
                shutdown == null ? null : shutdown.getNodeId()
            );
        } else if (isReplacementSource(allocation, node.getId())) {
            return Decision.single(
                Decision.Type.NO,
                NAME,
                "node [%s] is being replaced by [%s], shards cannot auto expand to be on it",
                node.getId(),
                getReplacementName(allocation, node.getId())
            );
        } else {
            return Decision.single(
                Decision.Type.YES,
                NAME,
                "node is not part of a node replacement, so shards may be auto expanded onto it"
            );
        }
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (replacementFromSourceToTarget(allocation, shardRouting.currentNodeId(), node.node().getName())) {
            return Decision.single(
                Decision.Type.YES,
                NAME,
                "node [%s] is being replaced by node [%s], and can be force vacated to the target",
                shardRouting.currentNodeId(),
                node.nodeId()
            );
        } else {
            return Decision.single(
                Decision.Type.NO,
                NAME,
                "shard is not on the source of a node replacement relocated to the replacement target"
            );
        }
    }

    @Override
    public Decision canAllocateReplicaWhenThereIsRetentionLease(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (isReplacementTargetName(allocation, node.node().getName())) {
            return Decision.single(
                Decision.Type.YES,
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
        final SingleNodeShutdownMetadata shutdown = allocation.nodeShutdowns().get(sourceNodeId);
        return shutdown != null
            && shutdown.getType().equals(SingleNodeShutdownMetadata.Type.REPLACE)
            && shutdown.getNodeId().equals(sourceNodeId)
            && shutdown.getTargetNodeName().equals(targetNodeName);
    }

    /**
     * Returns true if the given node id is the source (the replaced node) of an ongoing node replacement
     */
    private static boolean isReplacementSource(RoutingAllocation allocation, String nodeId) {
        if (nodeId == null || replacementOngoing(allocation) == false) {
            return false;
        }
        final Map<String, SingleNodeShutdownMetadata> nodeShutdowns = allocation.nodeShutdowns();
        return nodeShutdowns.containsKey(nodeId) && nodeShutdowns.get(nodeId).getType().equals(SingleNodeShutdownMetadata.Type.REPLACE);
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
        return Optional.ofNullable(allocation.nodeShutdowns().get(nodeIdBeingReplaced))
            .filter(shutdown -> shutdown.getType().equals(SingleNodeShutdownMetadata.Type.REPLACE))
            .map(SingleNodeShutdownMetadata::getTargetNodeName)
            .orElse(null);
    }
}
