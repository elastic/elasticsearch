/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.EstimatedHeapUsage;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import static org.elasticsearch.xpack.stateless.allocation.EstimatedHeapUsageAllocationDecider.NAME;

public class ExistingEstimatedHeapUsageAllocationDeciderNodeThingo implements EstimatedHeapUsageAllocationDeciderNodeThingo {

    private static final Decision YES_ESTIMATED_HEAP_USAGE_FOR_INDEX_NODE_ONLY = Decision.single(
        Decision.Type.YES,
        NAME,
        "estimated heap allocation decider is applicable only to index nodes"
    );

    @Override
    public Decision decisionForNode(RoutingNode node) {
        if (node.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
            return YES_ESTIMATED_HEAP_USAGE_FOR_INDEX_NODE_ONLY;
        }
        return null;
    }

    @Override
    public EstimatedHeapUsage heapUsageForNode(RoutingAllocation allocation, RoutingNode node) {
        return allocation.clusterInfo().getEstimatedHeapUsages().get(node.nodeId());
    }
}
