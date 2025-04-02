/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.Collection;
import java.util.List;

/**
 * Produces {@link PartitionedNodeSorter}s that treat the cluster as a single partition.
 */
public class GlobalNodeSorterFactory implements NodeSorterFactory {

    private final BalancerSettings balancerSettings;

    public GlobalNodeSorterFactory(BalancerSettings balancerSettings) {
        this.balancerSettings = balancerSettings;
    }

    @Override
    public PartitionedNodeSorter create(BalancedShardsAllocator.ModelNode[] allNodes, BalancedShardsAllocator.Balancer balancer) {
        final WeightFunction weightFunction = new WeightFunction(
            balancerSettings.getShardBalanceFactor(),
            balancerSettings.getIndexBalanceFactor(),
            balancerSettings.getWriteLoadBalanceFactor(),
            balancerSettings.getDiskUsageBalanceFactor()
        );
        return new GlobalNodeSorter(new BalancedShardsAllocator.NodeSorter(allNodes, weightFunction, balancer));
    }

    private record GlobalNodeSorter(BalancedShardsAllocator.NodeSorter nodeSorter) implements PartitionedNodeSorter {

        @Override
        public Collection<BalancedShardsAllocator.NodeSorter> allNodeSorters() {
            return List.of(nodeSorter);
        }

        @Override
        public BalancedShardsAllocator.NodeSorter sorterForShard(ShardRouting shard) {
            return nodeSorter;
        }

        @Override
        public BalancedShardsAllocator.NodeSorter sorterForNode(BalancedShardsAllocator.ModelNode node) {
            return nodeSorter;
        }
    }
}
