/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MetricCollectorsTests extends ESTestCase {

    public void testEmptyPartitionSizeCollector() {
        final PlainActionFuture<Map<String, Long>> future = new PlainActionFuture<>();
        PartitionSizeCollector.EMPTY.collectHostedShardsPartitionSizes(ClusterState.EMPTY_STATE, future);
        assertThat(safeGet(future), equalTo(Map.of()));
    }

    public void testEmptySearchLaneRequirementsCollector() {
        final PlainActionFuture<Map<ShardId, Double>> future = new PlainActionFuture<>();
        SearchLaneRequirementsCollector.EMPTY.collectSearchLaneRequirements(ClusterState.EMPTY_STATE, future);
        assertThat(safeGet(future), equalTo(Map.of()));
    }

    public void testEmptyEstimatedHeapUsageCollector() {
        final PlainActionFuture<EstimatedHeapUsageStats> future = new PlainActionFuture<>();
        EstimatedHeapUsageCollector.EMPTY.collectEstimatedHeapUsage(future);
        final EstimatedHeapUsageStats stats = safeGet(future);
        assertThat(stats.nodeHeapEstimates(), equalTo(Map.of()));
        assertThat(stats.shardHeapUsageEstimates(), equalTo(ShardHeapUsageEstimates.empty()));
    }

    public void testEmptySearchLoadMetricsCollector() {
        final PlainActionFuture<SearchLoadMetrics> future = new PlainActionFuture<>();
        SearchLoadMetricsCollector.EMPTY.collectSearchLoadMetrics(ClusterState.EMPTY_STATE, future);
        final SearchLoadMetrics metrics = safeGet(future);
        assertThat(metrics.searchCopyDemands(), equalTo(Map.of()));
        assertThat(metrics.nodeSearchCapacities(), equalTo(Map.of()));
    }

    public void testEmptyCacheSizesAndCommitmentCollector() {
        final PlainActionFuture<CacheSizesAndCommitmentStats> future = new PlainActionFuture<>();
        CacheSizesAndCommitmentCollector.EMPTY.collectCacheSizesAndCommitmentStats(ClusterState.EMPTY_STATE, future);
        final CacheSizesAndCommitmentStats stats = safeGet(future);
        assertThat(stats.shardCacheRequirements(), equalTo(Map.of()));
        assertThat(stats.nodeCacheSizeAndCommitments(), equalTo(Map.of()));
    }
}
