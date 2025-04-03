/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;

/**
 * A partitioned cluster factory must be able to divide all shards and nodes into mutually
 * disjoint partitions. Allocation balancing will then be conducted sequentially for each partition.
 * <p>
 * If you can't partition your shards and nodes in this way, use
 * {@link org.elasticsearch.cluster.routing.allocation.allocator.GlobalPartitionedClusterFactory}
 */
public interface PartitionedClusterFactory {

    PartitionedCluster create(
        WriteLoadForecaster writeLoadForecaster,
        ClusterInfo clusterInfo,
        Metadata metadata,
        RoutingNodes routingNodes
    );

    default PartitionedCluster create(WriteLoadForecaster writeLoadForecaster, RoutingAllocation allocation) {
        return create(writeLoadForecaster, allocation.clusterInfo(), allocation.metadata(), allocation.routingNodes());
    }
}
