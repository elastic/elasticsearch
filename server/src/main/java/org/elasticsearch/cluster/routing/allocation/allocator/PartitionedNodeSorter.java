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

/**
 * A partitioned node sorter must be able to divider all shards and nodes into mutually
 * disjoint partitions. Allocation balancing is then conducted sequentially for each partition.
 * <p>
 * If you can't partition your shards and nodes in this way, use
 * {@link org.elasticsearch.cluster.routing.allocation.allocator.GlobalNodeSorterFactory}
 */
public interface PartitionedNodeSorter {

    /**
     * Get the {@link BalancedShardsAllocator.NodeSorter}s for all partitions
     */
    Collection<BalancedShardsAllocator.NodeSorter> allNodeSorters();

    /**
     * Get the {@link BalancedShardsAllocator.NodeSorter} for the specified shard
     */
    BalancedShardsAllocator.NodeSorter sorterForShard(ShardRouting shard);

    /**
     * Get the {@link BalancedShardsAllocator.NodeSorter} for the specified node
     */
    BalancedShardsAllocator.NodeSorter sorterForNode(BalancedShardsAllocator.ModelNode node);
}
