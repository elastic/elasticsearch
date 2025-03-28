/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;

/**
 * The set of "node types" that are significant for balancer weight calculations.
 */
enum NodeType {
    INDEXING,
    SEARCH,
    OTHER;

    static NodeType forNode(RoutingNode node) {
        final DiscoveryNode discoveryNode = node.node();
        if (discoveryNode == null) {
            return NodeType.OTHER;
        } else if (discoveryNode.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE)) {
            return INDEXING;
        } else if (discoveryNode.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE)) {
            return SEARCH;
        } else {
            return NodeType.OTHER;
        }
    }
}
