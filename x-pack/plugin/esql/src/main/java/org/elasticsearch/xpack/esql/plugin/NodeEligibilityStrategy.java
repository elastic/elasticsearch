/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Determines which nodes are eligible to execute external-source splits.
 */
public interface NodeEligibilityStrategy {

    List<DiscoveryNode> eligibleNodes(DiscoveryNodes allNodes);

    NodeEligibilityStrategy ALL_NODES = allNodes -> filterNodes(allNodes, n -> true);

    NodeEligibilityStrategy DATA_NODES_ONLY = allNodes -> filterNodes(allNodes, DiscoveryNode::canContainData);

    /**
     * Matches only nodes that carry the {@code search} role. In stateless deployments, search nodes host the shared
     * blob cache and are the correct target for external-file scans; index nodes (write path) lack the cache and
     * would read every split cold from object storage.
     */
    NodeEligibilityStrategy SEARCH_NODES_ONLY = allNodes -> filterNodes(
        allNodes,
        node -> node.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName())
    );

    private static List<DiscoveryNode> filterNodes(DiscoveryNodes allNodes, Predicate<DiscoveryNode> predicate) {
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (DiscoveryNode node : allNodes) {
            if (predicate.test(node)) {
                nodes.add(node);
            }
        }
        return nodes;
    }
}
