/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.Collection;
import java.util.List;

public class GlobalPartitionedClusterFactory implements PartitionedClusterFactory {

    private final BalancerSettings balancerSettings;

    public GlobalPartitionedClusterFactory(BalancerSettings balancerSettings) {
        this.balancerSettings = balancerSettings;
    }

    @Override
    public PartitionedCluster create() {
        return new GlobalPartitionedCluster();
    }

    private class GlobalPartitionedCluster implements PartitionedCluster {

        private final WeightFunction weightFunction;

        GlobalPartitionedCluster() {
            this.weightFunction = new WeightFunction(
                balancerSettings.getShardBalanceFactor(),
                balancerSettings.getIndexBalanceFactor(),
                balancerSettings.getWriteLoadBalanceFactor(),
                balancerSettings.getDiskUsageBalanceFactor()
            );
        }

        @Override
        public WeightFunction weightFunctionForShard(ShardRouting shard) {
            return weightFunction;
        }

        @Override
        public WeightFunction weightFunctionForNode(RoutingNode node) {
            return weightFunction;
        }

        public PartitionedNodeSorter createPartitionedNodeSorter(
            BalancedShardsAllocator.ModelNode[] modelNodes,
            BalancedShardsAllocator.Balancer balancer
        ) {
            return new GlobalPartitionedNodeSorter(new BalancedShardsAllocator.NodeSorter(modelNodes, weightFunction, balancer));
        }

        private record GlobalPartitionedNodeSorter(BalancedShardsAllocator.NodeSorter nodeSorter) implements PartitionedNodeSorter {

            @Override
            public Collection<BalancedShardsAllocator.NodeSorter> allNodeSorters() {
                return List.of(nodeSorter);
            }

            @Override
            public BalancedShardsAllocator.NodeSorter sorterForShard(ShardRouting shard) {
                return nodeSorter;
            }
        }
    }
}
