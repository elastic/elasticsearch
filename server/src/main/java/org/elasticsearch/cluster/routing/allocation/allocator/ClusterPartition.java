/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

public interface ClusterPartition {

    WeightFunction weightFunction();

    float avgShardsPerNode(BalancedShardsAllocator.ProjectIndex index);

    float avgShardsPerNode();

    double avgWriteLoadPerNode();

    double avgDiskUsageInBytesPerNode();

    default float calculateNodeWeight(int numShards, double nodeWriteLoad, double diskUsageInBytes) {
        return weightFunction().calculateNodeWeight(numShards, nodeWriteLoad, diskUsageInBytes);
    }

    default float calculateNodeWeightWithIndex(BalancedShardsAllocator.ModelNode node, BalancedShardsAllocator.ProjectIndex index) {
        return weightFunction().calculateNodeWeightWithIndex(node, index);
    }

    default float minWeightDelta(float shardWriteLoad, float shardSizeBytes) {
        return weightFunction().minWeightDelta(shardWriteLoad, shardSizeBytes);
    }
}
