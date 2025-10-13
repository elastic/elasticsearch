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
import org.elasticsearch.common.collect.Iterators;

import java.util.Iterator;

public class GlobalBalancingWeightsFactory implements BalancingWeightsFactory {

    private final BalancerSettings balancerSettings;

    public GlobalBalancingWeightsFactory(BalancerSettings balancerSettings) {
        this.balancerSettings = balancerSettings;
    }

    @Override
    public BalancingWeights create() {
        return new GlobalBalancingWeights();
    }

    private class GlobalBalancingWeights implements BalancingWeights {

        private final WeightFunction weightFunction;

        GlobalBalancingWeights() {
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

        @Override
        public NodeSorters createNodeSorters(BalancedShardsAllocator.ModelNode[] modelNodes, BalancedShardsAllocator.Balancer balancer) {
            return new GlobalNodeSorters(new BalancedShardsAllocator.NodeSorter(modelNodes, weightFunction, balancer));
        }

        private record GlobalNodeSorters(BalancedShardsAllocator.NodeSorter nodeSorter) implements NodeSorters {

            @Override
            public BalancedShardsAllocator.NodeSorter sorterForShard(ShardRouting shard) {
                return nodeSorter;
            }

            @Override
            public Iterator<BalancedShardsAllocator.NodeSorter> iterator() {
                return Iterators.single(nodeSorter);
            }
        }
    }
}
