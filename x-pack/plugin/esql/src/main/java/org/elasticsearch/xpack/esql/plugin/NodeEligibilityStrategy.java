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

/**
 * Determines which nodes are eligible to execute external-source splits as remote workers.
 */
public interface NodeEligibilityStrategy {

    List<DiscoveryNode> eligibleNodes(DiscoveryNodes allNodes);

    /**
     * Data-capable nodes that do not carry {@link DiscoveryNodeRole#INDEX_ROLE}.
     * <p>
     * An index node is never a valid remote scan worker: sending external scan work there risks
     * OOM-ing the indexing tier, and an OOM-ed index node affects ingest for everything on that
     * node. Coordinator-local execution remains allowed -- this predicate is only consulted when
     * a strategy is choosing remote workers.
     * <p>
     * An empty result is a normal outcome, not an error. On an index-only cluster the strategies
     * fall back to {@code LOCAL} and the coordinator runs the scan itself, which is what keeps a
     * query alive when the controller routes it to an index node with no search capacity left.
     * <p>
     * The test is data-capability minus {@code INDEX_ROLE} rather than "has {@code SEARCH_ROLE}":
     * both {@code INDEX_ROLE} and {@code SEARCH_ROLE} are declared with {@code canContainData == true},
     * and requiring {@code SEARCH_ROLE} would silently disable distribution on every stateful
     * cluster. Stateful nodes do not carry {@code INDEX_ROLE}, so they remain eligible.
     * <p>
     * {@link DiscoveryNode#hasRole(String)} directly expresses role-name membership;
     * {@code DiscoveryNode} already canonicalizes roles known to the receiving version.
     */
    NodeEligibilityStrategy EXTERNAL_WORKER_NODES = allNodes -> {
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (DiscoveryNode node : allNodes) {
            if (node.canContainData() && node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()) == false) {
                nodes.add(node);
            }
        }
        return nodes;
    };
}
