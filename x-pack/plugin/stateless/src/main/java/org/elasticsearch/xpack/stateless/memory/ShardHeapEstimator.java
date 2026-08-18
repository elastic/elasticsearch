/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.common.unit.ByteSizeValue;

import static org.elasticsearch.xpack.stateless.memory.ShardMappingSize.UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;

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
    private final boolean includePostingsInEstimate;

    public ShardHeapEstimator(
        ByteSizeValue fixedShardMemoryOverhead,
        double adaptiveExtraOverheadRatio,
        long adaptiveShardMemoryEstimationMinThreshold,
        boolean selfReportedShardMemoryOverheadEnabled,
        boolean includePostingsInEstimate
    ) {
        this.fixedShardMemoryOverhead = fixedShardMemoryOverhead;
        this.adaptiveExtraOverheadRatio = adaptiveExtraOverheadRatio;
        this.adaptiveShardMemoryEstimationMinThreshold = adaptiveShardMemoryEstimationMinThreshold;
        this.selfReportedShardMemoryOverheadEnabled = selfReportedShardMemoryOverheadEnabled;
        this.includePostingsInEstimate = includePostingsInEstimate;
    }

    /**
     * Computes the shard-level heap usage: the self-reported overhead if {@link #selfReportedShardMemoryOverheadEnabled} is true and
     * there is one available, otherwise {@link #estimateShardOverheadExcludingPostings} adding postings if
     * {@link #includePostingsInEstimate} is true.
     * <p>
     * Ignores index-level heap usage, {@link StatelessMemoryMetricsService#computeIndexHeapUsage} should be called for that.
     */
    public long computeShardHeapUsage(StatelessMemoryMetricsService.ShardMemoryMetrics shardMemoryMetrics) {
        if (isSelfReportedShardMemoryOverheadAvailable(shardMemoryMetrics)) {
            return shardMemoryMetrics.getShardMemoryOverheadBytes();
        }
        final long postingsMemoryInBytes = includePostingsInEstimate ? shardMemoryMetrics.getPostingsInMemoryBytes() : 0;
        return estimateShardOverheadExcludingPostings(shardMemoryMetrics) + postingsMemoryInBytes;
    }

    /**
     * Estimates a shard's fixed/adaptive memory overhead (segment, field, live-doc byte counts, and points memory metrics),
     * <b>excluding</b> postings memory ({@link StatelessMemoryMetricsService.ShardMemoryMetrics#getPostingsInMemoryBytes()});
     */
    private long estimateShardOverheadExcludingPostings(StatelessMemoryMetricsService.ShardMemoryMetrics metrics) {
        final var fixedShardOverhead = this.fixedShardMemoryOverhead;
        if (fixedShardOverhead.getBytes() > 0) {
            return fixedShardOverhead.getBytes();
        }
        long estimateBytes = ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + metrics.getNumSegments() * ADAPTIVE_SEGMENT_MEMORY_OVERHEAD
            .getBytes() + metrics.getTotalFields() * ADAPTIVE_FIELD_MEMORY_OVERHEAD.getBytes() + metrics.getLiveDocsBytes() + metrics
                .getPointsInMemoryBytes();
        long extraBytes = (long) (estimateBytes * adaptiveExtraOverheadRatio);

        return Math.max(adaptiveShardMemoryEstimationMinThreshold, estimateBytes + extraBytes);
    }

    private boolean isSelfReportedShardMemoryOverheadAvailable(StatelessMemoryMetricsService.ShardMemoryMetrics shardMemoryMetrics) {
        return selfReportedShardMemoryOverheadEnabled
            && shardMemoryMetrics.getShardMemoryOverheadBytes() != UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
    }
}
