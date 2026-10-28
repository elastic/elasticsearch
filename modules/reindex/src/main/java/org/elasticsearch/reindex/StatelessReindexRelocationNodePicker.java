/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.List;
import java.util.Optional;

class StatelessReindexRelocationNodePicker implements ReindexRelocationNodePicker {

    private static final Logger logger = LogManager.getLogger(StatelessReindexRelocationNodePicker.class);

    StatelessReindexRelocationNodePicker() {}

    @Override
    public Optional<String> pickNode(DiscoveryNodes nodes, NodesShutdownMetadata nodeShutdowns) {
        String currentNodeId = nodes.getLocalNodeId();
        if (currentNodeId == null) {
            logger.warn(
                "Trying to pick a node to relocate a reindex task to, but the current node ID is unexpectedly unknown:"
                    + " the relocation attempt will be aborted"
            );
            return Optional.empty();
        }
        // Many stateless configurations won't have dedicated coordinating nodes - but if they exist, we choose them over indexing nodes:
        List<String> eligibleDedicatedCoordinatingNodes = nodes.getNodes()
            .values()
            .stream()
            .filter(node -> node.getRoles().isEmpty())
            .map(DiscoveryNode::getId)
            .filter(id -> id.equals(currentNodeId) == false)
            .filter(id -> nodeShutdowns.contains(id) == false)
            .toList();
        if (eligibleDedicatedCoordinatingNodes.isEmpty() == false) {
            String newNodeId = selectRandomNodeIdFrom(eligibleDedicatedCoordinatingNodes);
            logger.debug("Chose dedicated coordinating node ID {} for relocating a reindex task from node {}", newNodeId, currentNodeId);
            return Optional.of(newNodeId);
        }
        List<String> eligibleIndexingNodes = nodes.getNodes()
            .values()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
            .map(DiscoveryNode::getId)
            .filter(id -> id.equals(currentNodeId) == false)
            .filter(id -> nodeShutdowns.contains(id) == false)
            .toList();
        if (eligibleIndexingNodes.isEmpty() == false) {
            String newNodeId = selectRandomNodeIdFrom(eligibleIndexingNodes);
            logger.debug("Chose indexing node node ID {} for relocating a reindex task from node {}", newNodeId, currentNodeId);
            return Optional.of(newNodeId);
        }
        logger.debug(
            "Trying to pick a node to relocate a reindex task to, but there are no dedicated coordinating or indexing nodes "
                + "(perhaps excluding the current node): the relocation attempt will be aborted"
        );
        return Optional.empty();
    }

    private String selectRandomNodeIdFrom(List<String> nodeIds) {
        return nodeIds.get(Randomness.get().nextInt(nodeIds.size()));
    }
}
