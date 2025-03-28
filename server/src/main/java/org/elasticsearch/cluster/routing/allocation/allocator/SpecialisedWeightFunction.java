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
import org.elasticsearch.cluster.routing.allocation.BalancedAllocatorSettings;

import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.ModelNode.NodeType.INDEXING;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.ModelNode.NodeType.SEARCH;

public class SpecialisedWeightFunction implements WeightFunction {

    private final SingleWeightFunction defaultWeightFunction;
    private final SingleWeightFunction indexingWeightFunction;
    private final SingleWeightFunction searchWeightFunction;

    public SpecialisedWeightFunction(BalancedAllocatorSettings balancedAllocatorSettings) {
        this.defaultWeightFunction = new SingleWeightFunction(
            balancedAllocatorSettings.getShardBalanceFactor(),
            balancedAllocatorSettings.getIndexBalanceFactor(),
            balancedAllocatorSettings.getWriteLoadBalanceFactor(),
            balancedAllocatorSettings.getDiskUsageBalanceFactor()
        );
        this.indexingWeightFunction = new SingleWeightFunction(
            balancedAllocatorSettings.getIndexingTierShardBalanceFactor(),
            balancedAllocatorSettings.getIndexBalanceFactor(),
            balancedAllocatorSettings.getIndexingTierWriteLoadBalanceFactor(),
            balancedAllocatorSettings.getDiskUsageBalanceFactor()
        );
        this.searchWeightFunction = new SingleWeightFunction(
            balancedAllocatorSettings.getSearchTierShardBalanceFactor(),
            balancedAllocatorSettings.getIndexBalanceFactor(),
            balancedAllocatorSettings.getSearchTierWriteLoadBalanceFactor(),
            balancedAllocatorSettings.getDiskUsageBalanceFactor()
        );
    }

    @Override
    public float calculateNodeWeightWithIndex(
        BalancedShardsAllocator.Balancer balancer,
        BalancedShardsAllocator.ModelNode node,
        BalancedShardsAllocator.ProjectIndex index
    ) {
        return weightFunctionForType(node.nodeType()).calculateNodeWeightWithIndex(balancer, node, index);
    }

    @Override
    public float calculateNodeWeight(
        RoutingNode node,
        int nodeNumShards,
        float avgShardsPerNode,
        double nodeWriteLoad,
        double avgWriteLoadPerNode,
        double diskUsageInBytes,
        double avgDiskUsageInBytesPerNode
    ) {
        return weightFunctionForType(nodeTypeForNode(node)).calculateNodeWeight(
            node,
            nodeNumShards,
            avgShardsPerNode,
            nodeWriteLoad,
            avgWriteLoadPerNode,
            diskUsageInBytes,
            avgDiskUsageInBytesPerNode
        );
    }

    @Override
    public float minWeightDelta(BalancedShardsAllocator.ModelNode modelNode, float shardWriteLoad, float shardSizeBytes) {
        return weightFunctionForType(modelNode.nodeType()).minWeightDelta(modelNode, shardWriteLoad, shardSizeBytes);
    }

    private BalancedShardsAllocator.ModelNode.NodeType nodeTypeForNode(RoutingNode node) {
        final DiscoveryNode discoveryNode = node.node();
        if (discoveryNode == null) {
            return BalancedShardsAllocator.ModelNode.NodeType.UNKNOWN;
        } else if (discoveryNode.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE)) {
            return INDEXING;
        } else if (discoveryNode.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE)) {
            return SEARCH;
        } else {
            return BalancedShardsAllocator.ModelNode.NodeType.UNKNOWN;
        }
    }

    private WeightFunction weightFunctionForType(BalancedShardsAllocator.ModelNode.NodeType nodeType) {
        return switch (nodeType) {
            case SEARCH -> searchWeightFunction;
            case INDEXING -> indexingWeightFunction;
            default -> defaultWeightFunction;
        };
    }
}
