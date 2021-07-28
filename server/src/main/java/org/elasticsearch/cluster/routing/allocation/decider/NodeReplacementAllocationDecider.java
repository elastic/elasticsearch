/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

public class NodeReplacementAllocationDecider extends AllocationDecider {

    public static final String NAME = "node_replacement";

    static final Decision NO_REPLACEMENTS = Decision.single(Decision.Type.YES, NAME,
        "no node replacements are currently ongoing, allocation is allowed");

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final Metadata metadata = allocation.metadata();
        if (replacementOngoing(metadata) == false) {
            return NO_REPLACEMENTS;
        } else if (replacementFromSourceToTarget(metadata, shardRouting.currentNodeId(), node.nodeId())) {
            return Decision.single(Decision.Type.YES, NAME,
                "node [%s] is replacing node [%s], and may receive shards from it", shardRouting.currentNodeId(), node.nodeId());
        } else if (isReplacementSource(metadata, shardRouting.currentNodeId())) {
            return Decision.single(Decision.Type.NO, NAME,
                "node [%s] is being replaced, and its shards may only be allocated to the replacement target",
                shardRouting.currentNodeId());
        } else if (isReplacementSource(metadata, node.nodeId())) {
            return Decision.single(Decision.Type.NO, NAME,
                "node [%s] is being removed, so no data from other nodes may be allocated to it", node.nodeId());
        } else if (isReplacementTargetName(metadata, node.node().getName())) {
            return Decision.single(Decision.Type.NO, NAME,
                "node [%s] is replacing a vacating node, so no data from other nodes " +
                    "may be allocated to it until the replacement is complete", node.nodeId());
        } else {
            return Decision.single(Decision.Type.YES, NAME,
                "neither the source nor target node are part of an ongoing node replacement");
        }
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation.metadata()) == false) {
            return NO_REPLACEMENTS;
        } else if (isReplacementSource(allocation.metadata(), node.nodeId())) {
            return Decision.single(Decision.Type.NO, NAME,
                "node [%s] is being replaced by node [%s], so no data may remain on it", node.nodeId(),
                getReplacementName(allocation.metadata(), node.nodeId()));
        } else {
            return Decision.single(Decision.Type.YES, NAME, "node [%s] is not being replaced", node.nodeId());
        }
    }

    /**
     * See the comment in the else branch of {@link #canAllocate(ShardRouting, RoutingNode, RoutingAllocation)}
     * for a reason why we allow allocation that may potentially allocate to the source of a node
     * replacement shutdown.
     */
    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation.metadata()) == false) {
            return NO_REPLACEMENTS;
        } else if (isReplacementTargetName(allocation.metadata(), node.node().getName())) {
            return Decision.single(Decision.Type.NO, NAME,
                "node [%s] is replacing a vacating node, so no data from other nodes " +
                    "may be allocated to it until the replacement is complete", node.nodeId());
        } else {
            // The node in question is not a replacement target, so allow allocation.
            return Decision.single(Decision.Type.YES, NAME,
                "node is not a replacement target, so allocation is allowed");
        }
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation.metadata()) == false) {
            return NO_REPLACEMENTS;
        } else if (isReplacementTargetName(allocation.metadata(), node.getName())) {
            return Decision.single(Decision.Type.NO, NAME,
                "node [%s] is a node replacement target, shards cannot auto expand to be on it until the replacement is complete",
                node.getId(), "source");
        } else if (isReplacementSource(allocation.metadata(), node.getId())) {
            return Decision.single(Decision.Type.NO, NAME,
                "node [%s] is being replaced, shards cannot auto expand to be on it", node.getId());
        } else {
            return Decision.single(Decision.Type.YES, NAME,
                "node is not part of a node replacement, so shards may be auto expanded onto it");
        }
    }

    @Override
    public Decision canForceDuringVacate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (replacementFromSourceToTarget(allocation.metadata(), shardRouting.currentNodeId(), node.nodeId())) {
            return Decision.single(Decision.Type.YES, NAME,
                "node [%s] is being replaced by node [%s], and can be force vacated to the target",
                shardRouting.currentNodeId(), node.nodeId());
        } else {
            return Decision.single(Decision.Type.NO, NAME,
                "shard is not on the source of a node replacement relocated to the replacement target");
        }
    }

    /**
     * Returns true if there are any node replacements ongoing in the cluster
     */
    private static boolean replacementOngoing(Metadata metadata) {
        return metadata.nodeShutdowns().values().stream()
            .anyMatch(shutdown -> shutdown.getType().equals(SingleNodeShutdownMetadata.Type.REPLACE));
    }

    /**
     * Returns true if there is a replacement currently ongoing from the source to the target node id
     */
    private static boolean replacementFromSourceToTarget(Metadata metadata, String sourceNodeId, String targetNodeId) {
        if (replacementOngoing(metadata) == false) {
            return false;
        }
        if (sourceNodeId == null || targetNodeId == null) {
            return false;
        }
        return metadata.nodeShutdowns().values().stream()
            .filter(shutdown -> shutdown.getType().equals(SingleNodeShutdownMetadata.Type.REPLACE))
            .filter(shutdown -> shutdown.getNodeId().equals(sourceNodeId))
            .anyMatch(shutdown -> shutdown.getTargetNodeName().equals(targetNodeId));
    }

    /**
     * Returns true if the given node id is the source (the replaced node) of an ongoing node replacement
     */
    private static boolean isReplacementSource(Metadata metadata, String nodeId) {
        if (nodeId == null || replacementOngoing(metadata) == false) {
            return false;
        }
        return metadata.nodeShutdowns().values().stream()
            .filter(shutdown -> shutdown.getType().equals(SingleNodeShutdownMetadata.Type.REPLACE))
            .anyMatch(shutdown -> shutdown.getNodeId().equals(nodeId));
    }

    /**
     * Returns true if the given node name (not the id!) is the target (the replacing node) of an ongoing node replacement
     */
    private static boolean isReplacementTargetName(Metadata metadata, String nodeName) {
        if (nodeName == null || replacementOngoing(metadata) == false) {
            return false;
        }
        return metadata.nodeShutdowns().values().stream()
            .filter(shutdown -> shutdown.getType().equals(SingleNodeShutdownMetadata.Type.REPLACE))
            .anyMatch(shutdown -> shutdown.getTargetNodeName().equals(nodeName));
    }

    private static String getReplacementName(Metadata metadata, String nodeBeingReplaced) {
        if (nodeBeingReplaced == null || replacementOngoing(metadata) == false) {
            return null;
        }
        return metadata.nodeShutdowns().values().stream()
            .filter(shutdown -> shutdown.getType().equals(SingleNodeShutdownMetadata.Type.REPLACE))
            .filter(shutdown -> shutdown.getNodeId().equals(nodeBeingReplaced))
            .findFirst()
            .map(SingleNodeShutdownMetadata::getTargetNodeName)
            .orElse(null);
    }
}
