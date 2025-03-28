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
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.ProjectIndex;

/**
 * This class is the primary weight function used to create balanced over nodes and shards in the cluster.
 * Currently this function has 3 properties:
 * <ul>
 * <li><code>index balance</code> - balance property over shards per index</li>
 * <li><code>shard balance</code> - balance property over shards per cluster</li>
 * </ul>
 * <p>
 * Each of these properties are expressed as factor such that the properties factor defines the relative
 * importance of the property for the weight function. For example if the weight function should calculate
 * the weights only based on a global (shard) balance the index balance can be set to {@code 0.0} and will
 * in turn have no effect on the distribution.
 * </p>
 * The weight per index is calculated based on the following formula:
 * <ul>
 * <li>
 * <code>weight<sub>index</sub>(node, index) = indexBalance * (node.numShards(index) - avgShardsPerNode(index))</code>
 * </li>
 * <li>
 * <code>weight<sub>node</sub>(node, index) = shardBalance * (node.numShards() - avgShardsPerNode)</code>
 * </li>
 * </ul>
 * <code>weight(node, index) = weight<sub>index</sub>(node, index) + weight<sub>node</sub>(node, index)</code>
 */
public class SingleWeightFunction implements WeightFunction {

    private final float theta0;
    private final float theta1;
    private final float theta2;
    private final float theta3;

    public SingleWeightFunction(float shardBalance, float indexBalance, float writeLoadBalance, float diskUsageBalance) {
        float sum = shardBalance + indexBalance + writeLoadBalance + diskUsageBalance;
        if (sum <= 0.0f) {
            throw new IllegalArgumentException("Balance factors must sum to a value > 0 but was: " + sum);
        }
        theta0 = shardBalance / sum;
        theta1 = indexBalance / sum;
        theta2 = writeLoadBalance / sum;
        theta3 = diskUsageBalance / sum;
    }

    @Override
    public float calculateNodeWeightWithIndex(
        BalancedShardsAllocator.Balancer balancer,
        BalancedShardsAllocator.ModelNode node,
        ProjectIndex index
    ) {
        final float weightIndex = node.numShards(index) - balancer.avgShardsPerNode(index);
        final float nodeWeight = calculateNodeWeight(
            node.getRoutingNode(),
            node.numShards(),
            balancer.avgShardsPerNode(),
            node.writeLoad(),
            balancer.avgWriteLoadPerNode(),
            node.diskUsageInBytes(),
            balancer.avgDiskUsageInBytesPerNode()
        );
        return nodeWeight + theta1 * weightIndex;
    }

    @Override
    public float calculateNodeWeight(
        RoutingNode routingNode,
        int nodeNumShards,
        float avgShardsPerNode,
        double nodeWriteLoad,
        double avgWriteLoadPerNode,
        double diskUsageInBytes,
        double avgDiskUsageInBytesPerNode
    ) {
        final float weightShard = nodeNumShards - avgShardsPerNode;
        final float ingestLoad = (float) (nodeWriteLoad - avgWriteLoadPerNode);
        final float diskUsage = (float) (diskUsageInBytes - avgDiskUsageInBytesPerNode);
        return theta0 * weightShard + theta2 * ingestLoad + theta3 * diskUsage;
    }

    @Override
    public float minWeightDelta(BalancedShardsAllocator.ModelNode modelNode, float shardWriteLoad, float shardSizeBytes) {
        return theta0 * 1 + theta1 * 1 + theta2 * shardWriteLoad + theta3 * shardSizeBytes;
    }
}
