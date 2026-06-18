/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory.partition;

/**
 * A snapshot of the raw signals used by {@link MemoryPartition} implementations to compute
 * their node heap requirements. Built by {@code StatelessMemoryMetricsService.buildPartitionContext()}.
 *
 * @param totalIndices                            Total number of indices in the cluster.
 * @param largestMergeEstimateBytes               Memory estimate for the largest active or queued merge.
 * @param minimumRequiredHeapForIndexingOpsBytes  Minimum heap required to accept the largest recent
 *                                               indexing operation, already expressed as a heap
 *                                               requirement (i.e. largest_doc / 0.10 per the
 *                                               IndexingPressure single-document constraint).
 * @param largestShardCostBytes                   Memory cost of the most expensive single shard to host.
 * @param totalShardCostBytes                     Total memory cost of all shards in the cluster
 *                                               (the tier-level workload).
 */
public record PartitionContext(
    int totalIndices,
    long largestMergeEstimateBytes,
    long minimumRequiredHeapForIndexingOpsBytes,
    long largestShardCostBytes,
    long totalShardCostBytes
) {}
