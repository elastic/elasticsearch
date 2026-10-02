/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;

import java.util.List;
import java.util.Optional;

class StatefulReindexRelocationNodePicker implements ReindexRelocationNodePicker {

    private static final Logger logger = LogManager.getLogger(StatefulReindexRelocationNodePicker.class);

    StatefulReindexRelocationNodePicker() {}

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
        List<String> eligibleDataNodes = nodes.getDataNodes()
            .keySet()
            .stream()
            .filter(id -> id.equals(currentNodeId) == false)
            .filter(id -> nodeShutdowns.contains(id) == false)
            .toList();
        if (eligibleDataNodes.isEmpty() == false) {
            String newNodeId = selectRandomNodeIdFrom(eligibleDataNodes);
            logger.debug(
                "Chose data node ID {} for relocating a reindex task from node {}"
                    + " (there are no dedicated coordinating nodes, perhaps excluding the current node)",
                newNodeId,
                currentNodeId
            );
            return Optional.of(newNodeId);
        }
        logger.debug(
            "Trying to pick a node to relocate a reindex task to, but there are no dedicated coordinating or data nodes"
                + " (perhaps excluding the current node): the relocation attempt will be aborted"
        );
        return Optional.empty();
    }

    private String selectRandomNodeIdFrom(List<String> nodeIds) {
        return nodeIds.get(Randomness.get().nextInt(nodeIds.size()));
    }
}
