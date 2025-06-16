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

/**
 * The cluster nodes and shards are partitioned into mutually disjoint partitions. Each partition
 * has its own {@link WeightFunction}.
 */
public interface BalancingWeights {

    /**
     * Get the weight function for the partition to which this shard belongs
     *
     * @param shard The shard
     * @return The weight function that applies to the partition that shard belongs to
     */
    WeightFunction weightFunctionForShard(ShardRouting shard);

    /**
     * Get the weight function for the partition to which this node belongs
     *
     * @param node The node
     * @return The weight function that applies to the partition that node belongs to
     */
    WeightFunction weightFunctionForNode(RoutingNode node);

    /**
     * Create the node sorters for the cluster
     *
     * @param modelNodes The full set of cluster nodes
     * @param balancer The balancer
     * @return a {@link NodeSorters} instance
     */
    NodeSorters createNodeSorters(BalancedShardsAllocator.ModelNode[] modelNodes, BalancedShardsAllocator.Balancer balancer);
}
