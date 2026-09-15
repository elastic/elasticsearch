/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.MetricQuality;

import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.stateless.memory.ShardMappingSize.UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;

/// Encapsulates the logic for computing shard-level heap usage for individual shards or in aggregate.
///
/// Instances are constructed with the set of parameters that influence the heap-usage computation so
/// it can be used in a variety of contexts and reused for multiple calls.
public class ShardHeapEstimator {

    // The memory overhead of each IndexShard instance used in the adaptive estimate
    public static final ByteSizeValue ADAPTIVE_SHARD_MEMORY_OVERHEAD = ByteSizeValue.ofKb(75);
    // The memory overhead of each Lucene segment, including maps for postings, doc_values, and stored_fields producers
    public static final ByteSizeValue ADAPTIVE_SEGMENT_MEMORY_OVERHEAD = ByteSizeValue.ofKb(55);
    // The memory overhead of each field found in Lucene segments
    public static final ByteSizeValue ADAPTIVE_FIELD_MEMORY_OVERHEAD = ByteSizeValue.ofBytes(1024);

    private final ByteSizeValue fixedShardMemoryOverhead;
    private final double adaptiveExtraOverheadRatio;
    private final long adaptiveShardMemoryEstimationMinThreshold;
    private final boolean selfReportedShardMemoryOverheadEnabled;

    public ShardHeapEstimator(
        ByteSizeValue fixedShardMemoryOverhead,
        double adaptiveExtraOverheadRatio,
        long adaptiveShardMemoryEstimationMinThreshold,
        boolean selfReportedShardMemoryOverheadEnabled
    ) {
        this.fixedShardMemoryOverhead = fixedShardMemoryOverhead;
        this.adaptiveExtraOverheadRatio = adaptiveExtraOverheadRatio;
        this.adaptiveShardMemoryEstimationMinThreshold = adaptiveShardMemoryEstimationMinThreshold;
        this.selfReportedShardMemoryOverheadEnabled = selfReportedShardMemoryOverheadEnabled;
    }

    /// Compute the shard and index heap usage. The returned [ShardAndIndexHeapUsage] contains three values;
    /// - [ShardAndIndexHeapUsage#shardHeapUsageBytes()]
    /// - [ShardAndIndexHeapUsage#indexHeapUsageBytes()]
    /// - [ShardAndIndexHeapUsage#postingsHeapUsageBytes()]
    ///
    /// If [#selfReportedShardMemoryOverheadEnabled] and the shard has a self-reported overhead, it'll be returned in
    /// [ShardAndIndexHeapUsage#shardHeapUsageBytes()] and [ShardAndIndexHeapUsage#postingsHeapUsageBytes()] will be zero.
    ///
    /// If [#selfReportedShardMemoryOverheadEnabled] is disabled, or the shard has no self-reported overhead
    /// [ShardAndIndexHeapUsage#shardHeapUsageBytes()] will be set to the adaptive shard estimate and
    /// [ShardAndIndexHeapUsage#postingsHeapUsageBytes()] will be set to shard's reported postings size.
    public ShardAndIndexHeapUsage computeShardHeapUsage(StatelessMemoryMetricsService.ShardMemoryMetrics shardMemoryMetrics) {
        return new ShardAndIndexHeapUsage(
            computeShardHeapUsageIncludingPostings(shardMemoryMetrics),
            computeIndexHeapUsage(shardMemoryMetrics),
            getEffectiveShardPostingsInBytes(shardMemoryMetrics)
        );
    }

    /// Computes the shard-level heap usage: the self-reported overhead if [#selfReportedShardMemoryOverheadEnabled] is true and
    /// there is one available, otherwise returns the adaptive estimate including the postings.
    ///
    /// Ignores index-level heap usage, [#computeIndexHeapUsage] should be called for that.
    private long computeShardHeapUsageIncludingPostings(StatelessMemoryMetricsService.ShardMemoryMetrics shardMemoryMetrics) {
        if (isSelfReportedShardMemoryOverheadAvailable(shardMemoryMetrics)) {
            return shardMemoryMetrics.getShardMemoryOverheadBytes();
        }
        return estimateShardOverheadExcludingPostings(shardMemoryMetrics) + shardMemoryMetrics.getPostingsInMemoryBytes();
    }

    /// Get the "effective postings". If the shard doesn't have a self-reported overhead, or self-reported overheads are disabled,
    /// [StatelessMemoryMetricsService.ShardMemoryMetrics#getPostingsInMemoryBytes()] is returned. Otherwise, zero is returned
    /// because the self-reported overhead takes precedence.
    private long getEffectiveShardPostingsInBytes(StatelessMemoryMetricsService.ShardMemoryMetrics shardMemoryMetrics) {
        if (selfReportedShardMemoryOverheadEnabled == false
            || shardMemoryMetrics.getShardMemoryOverheadBytes() == UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES) {
            return shardMemoryMetrics.getPostingsInMemoryBytes();
        }
        return 0;
    }

    public record ShardMetricsAggregation(
        long mappingSizeInBytes,
        long totalShardHeapInBytes,
        long maxShardHeapInBytes,
        MetricQuality metricQuality
    ) {}

    /// Computes the index-level heap usage for a shard. [StatelessMemoryMetricsService#INDEX_MEMORY_OVERHEAD] is not included because
    /// all nodes include an overhead for all indices regardless of shard assignments: see
    /// [StatelessMemoryMetricsService#getNodeBaseHeapEstimateInBytes()].
    private long computeIndexHeapUsage(StatelessMemoryMetricsService.ShardMemoryMetrics shardMemoryMetrics) {
        return shardMemoryMetrics.getMappingSizeInBytes();
    }

    public ShardMetricsAggregation aggregateShardMetrics(
        Map<ShardId, StatelessMemoryMetricsService.ShardMemoryMetrics> shardMemoryMetrics,
        StatelessMemoryMetricsService.PostingsInEstimate postingsInEstimate
    ) {
        return aggregateShardMetrics(shardMemoryMetrics, postingsInEstimate, (shardId, metrics) -> {});
    }

    public ShardMetricsAggregation aggregateShardMetrics(
        Map<ShardId, StatelessMemoryMetricsService.ShardMemoryMetrics> shardMemoryMetrics,
        StatelessMemoryMetricsService.PostingsInEstimate postingsInEstimate,
        BiConsumer<ShardId, StatelessMemoryMetricsService.ShardMemoryMetrics> metricVisitor
    ) {
        long mappingSizeInBytes = 0;
        long totalShardHeapInBytes = 0;
        long maxShardHeapInBytes = 0;
        MetricQuality lowestMetricQuality = MetricQuality.EXACT;

        for (var entry : shardMemoryMetrics.entrySet()) {
            var metric = entry.getValue();
            var estimate = computeShardHeapUsage(metric);
            // Mapping overhead is incurred on each node that contains a shard from the index,
            // assume each shard is on a different node, so total overhead = num shards.
            // This will be an overestimate in either tier when there are fewer nodes than shards.
            mappingSizeInBytes += estimate.indexHeapUsageBytes();
            long shardHeap = postingsInEstimate == StatelessMemoryMetricsService.PostingsInEstimate.INCLUDE
                ? estimate.shardHeapUsageBytes()
                : estimate.shardHeapUsageBytesExcludingPostings();
            totalShardHeapInBytes += shardHeap;
            maxShardHeapInBytes = Math.max(maxShardHeapInBytes, shardHeap);
            if (metric.getMetricQuality().isLowerQualityThan(lowestMetricQuality)) {
                lowestMetricQuality = metric.getMetricQuality();
            }
            metricVisitor.accept(entry.getKey(), metric);
        }
        return new ShardMetricsAggregation(mappingSizeInBytes, totalShardHeapInBytes, maxShardHeapInBytes, lowestMetricQuality);
    }

    private long estimateShardOverheadExcludingPostings(StatelessMemoryMetricsService.ShardMemoryMetrics metrics) {
        final var fixedShardOverhead = this.fixedShardMemoryOverhead;
        if (fixedShardOverhead.getBytes() > 0) {
            return fixedShardOverhead.getBytes();
        }
        long estimateBytes = estimateShardOverheadExcludingPostings(metrics, adaptiveExtraOverheadRatio);
        return Math.max(adaptiveShardMemoryEstimationMinThreshold, estimateBytes);
    }

    private boolean isSelfReportedShardMemoryOverheadAvailable(StatelessMemoryMetricsService.ShardMemoryMetrics shardMemoryMetrics) {
        return selfReportedShardMemoryOverheadEnabled
            && shardMemoryMetrics.getShardMemoryOverheadBytes() != UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
    }

    /// This is package-private so it can be re-used in tests
    static long estimateShardOverheadExcludingPostings(
        StatelessMemoryMetricsService.ShardMemoryMetrics metrics,
        double adaptiveExtraOverheadRatio
    ) {
        long estimateBytes = ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + metrics.getNumSegments() * ADAPTIVE_SEGMENT_MEMORY_OVERHEAD
            .getBytes() + metrics.getTotalFields() * ADAPTIVE_FIELD_MEMORY_OVERHEAD.getBytes() + metrics.getLiveDocsBytes() + metrics
                .getPointsInMemoryBytes();
        long extraBytes = (long) (estimateBytes * adaptiveExtraOverheadRatio);
        return estimateBytes + extraBytes;
    }
}
