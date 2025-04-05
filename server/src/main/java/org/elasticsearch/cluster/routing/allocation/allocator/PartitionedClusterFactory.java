/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

/**
 * A partitioned cluster factory must be able to divide all shards and nodes into mutually
 * disjoint partitions. Allocation balancing will then be conducted sequentially for each partition.
 * <p>
 * If you can't partition your shards and nodes in this way, use
 * {@link org.elasticsearch.cluster.routing.allocation.allocator.GlobalPartitionedClusterFactory}
 */
public interface PartitionedClusterFactory {

    PartitionedCluster create();
}
