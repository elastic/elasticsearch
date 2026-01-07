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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;

import java.util.List;
import java.util.Random;

class DefaultReindexRelocationNodePicker implements ReindexRelocationNodePicker {

    private static final Logger logger = LogManager.getLogger(DefaultReindexRelocationNodePicker.class);

    private final Random random;

    DefaultReindexRelocationNodePicker() {
        this.random = Randomness.get();
    }

    @Override
    public String pickNode(DiscoveryNodes nodes) {
        String currentNodeId = nodes.getLocalNodeId();
        if (currentNodeId == null) {
            logger.debug(
                "Trying to pick a node to relocate a reindex task to, but the current node ID is unexpectedly unknown:"
                    + " the relocation attempt will be aborted"
            );
            return null;
        }
        List<String> eligibleDedicatedCoordinatingNodes = nodes.getNodes()
            .values()
            .stream()
            .filter(node -> node.getRoles().isEmpty())
            .map(DiscoveryNode::getId)
            .filter(id -> id.equals(currentNodeId) == false)
            .toList();
        if (eligibleDedicatedCoordinatingNodes.isEmpty() == false) {
            String newNodeId = randomNodeId(eligibleDedicatedCoordinatingNodes);
            logger.debug("Chose dedicated coordinating node ID {} for relocating a reindex task from node {}", newNodeId, currentNodeId);
            return newNodeId;
        }
        List<String> eligibleDataNodes = nodes.getDataNodes().keySet().stream().filter(id -> id.equals(currentNodeId) == false).toList();
        if (eligibleDataNodes.isEmpty() == false) {
            String newNodeId = randomNodeId(eligibleDataNodes);
            logger.debug(
                "Chose data node ID {} for relocating a reindex task from node {}"
                    + " (there are no dedicated coordinating nodes, perhaps excluding the current node)",
                newNodeId,
                currentNodeId
            );
            return newNodeId;
        }
        logger.debug(
            "Trying to pick a node to relocate a reindex task to, but there are no dedicated coordinating or data nodes"
                + " (perhaps excluding the current node): the relocation attempt will be aborted"
        );
        return null;
    }

    private String randomNodeId(List<String> nodeIds) {
        return nodeIds.get(random.nextInt(nodeIds.size()));
    }
}
