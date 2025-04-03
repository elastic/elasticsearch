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

public interface PartitionedCluster {

    ClusterPartition partitionForShard(ShardRouting shard);

    ClusterPartition partitionForNode(RoutingNode node);

    PartitionedNodeSorter createPartitionedNodeSorter(
        BalancedShardsAllocator.ModelNode[] modelNodes,
        BalancedShardsAllocator.Balancer balancer
    );
}
