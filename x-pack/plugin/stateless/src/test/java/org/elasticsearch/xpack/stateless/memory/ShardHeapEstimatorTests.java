/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.MetricQuality;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.stateless.memory.ShardHeapEstimator.ADAPTIVE_FIELD_MEMORY_OVERHEAD;
import static org.elasticsearch.xpack.stateless.memory.ShardHeapEstimator.ADAPTIVE_SEGMENT_MEMORY_OVERHEAD;
import static org.elasticsearch.xpack.stateless.memory.ShardHeapEstimator.ADAPTIVE_SHARD_MEMORY_OVERHEAD;
import static org.elasticsearch.xpack.stateless.memory.ShardMappingSize.UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ShardHeapEstimatorTests extends ESTestCase {

    // --- helpers ---

    private static StatelessMemoryMetricsService.ShardMemoryMetrics metrics(
        long mappingSizeInBytes,
        int numSegments,
        int totalFields,
        long postingsInMemoryBytes,
        long liveDocsBytes,
        long pointsInMemoryBytes,
        long shardMemoryOverheadBytes,
        MetricQuality metricQuality
    ) {
        return new StatelessMemoryMetricsService.ShardMemoryMetrics(
            mappingSizeInBytes,
            numSegments,
            totalFields,
            postingsInMemoryBytes,
            liveDocsBytes,
            pointsInMemoryBytes,
            shardMemoryOverheadBytes,
            0L,
            metricQuality,
            randomIdentifier(),
            System.nanoTime()
        );
    }

    /** Estimator with a fixed per-shard overhead and no self-reported overhead. */
    private static ShardHeapEstimator fixedEstimator(ByteSizeValue fixedOverhead) {
        return new ShardHeapEstimator(fixedOverhead, 0.0, 0L, false, false);
    }

    /** Estimator using the adaptive formula with no extra ratio, no min threshold, no self-reported overhead, postings excluded. */
    private static ShardHeapEstimator adaptiveEstimator(double extraRatio, long minThreshold) {
        return new ShardHeapEstimator(ByteSizeValue.ZERO, extraRatio, minThreshold, false, false);
    }

    // --- computeShardHeapUsage ---

    public void testFixedOverheadIsReturnedDirectly() {
        long fixedBytes = randomLongBetween(1, 10_000_000);
        ShardHeapEstimator estimator = fixedEstimator(ByteSizeValue.ofBytes(fixedBytes));
        var m = metrics(1000, 2, 10, 500, 200, 100, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        assertThat(estimator.computeShardHeapUsage(m), equalTo(fixedBytes));
    }

    /// This is a little surprising to me, and I'm pretty sure not right, but it's how it is on main,
    /// and I don't want to change behavior as part of a refactor.
    public void testFixedOverheadPlusPostingsWhenIncluded() {
        long fixedBytes = randomLongBetween(1, 10_000_000);
        long postings = randomLongBetween(1, 1_000_000);
        ShardHeapEstimator estimator = new ShardHeapEstimator(ByteSizeValue.ofBytes(fixedBytes), 0.0, 0L, false, true);
        var m = metrics(0, 1, 5, postings, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        assertThat(estimator.computeShardHeapUsage(m), equalTo(fixedBytes + postings));
    }

    public void testSelfReportedOverheadUsedWhenEnabledAndAvailable() {
        long selfReported = randomLongBetween(1, 10_000_000);
        ShardHeapEstimator estimator = new ShardHeapEstimator(ByteSizeValue.ZERO, 0.0, 0L, true, false);
        var m = metrics(0, 1, 5, 500, 100, 50, selfReported, MetricQuality.EXACT);
        assertThat(estimator.computeShardHeapUsage(m), equalTo(selfReported));
    }

    public void testSelfReportedOverheadIgnoredWhenDisabled() {
        long selfReported = randomLongBetween(1, 10_000_000);
        // selfReportedShardMemoryOverheadEnabled = false
        ShardHeapEstimator estimator = new ShardHeapEstimator(ByteSizeValue.ZERO, 0.0, 0L, false, false);
        var m = metrics(0, 0, 0, 0, 0, 0, selfReported, MetricQuality.EXACT);
        // should fall back to adaptive estimate (all zeros → ADAPTIVE_SHARD_MEMORY_OVERHEAD)
        assertThat(estimator.computeShardHeapUsage(m), equalTo(ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes()));
    }

    public void testSelfReportedOverheadIgnoredWhenUndefined() {
        ShardHeapEstimator estimator = new ShardHeapEstimator(ByteSizeValue.ZERO, 0.0, 0L, true, false);
        var m = metrics(0, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        // UNDEFINED → fall back to adaptive (all zeros → ADAPTIVE_SHARD_MEMORY_OVERHEAD)
        assertThat(estimator.computeShardHeapUsage(m), equalTo(ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes()));
    }

    public void testPostingsAddedWhenIncluded() {
        long postings = randomLongBetween(1, 1_000_000);
        ShardHeapEstimator estimator = new ShardHeapEstimator(ByteSizeValue.ZERO, 0.0, 0L, false, true);
        var m = metrics(0, 0, 0, postings, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        long expected = ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + postings;
        assertThat(estimator.computeShardHeapUsage(m), equalTo(expected));
    }

    public void testPostingsNotAddedWhenExcluded() {
        long postings = randomLongBetween(1, 1_000_000);
        ShardHeapEstimator estimator = new ShardHeapEstimator(ByteSizeValue.ZERO, 0.0, 0L, false, false);
        var m = metrics(0, 0, 0, postings, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        assertThat(estimator.computeShardHeapUsage(m), equalTo(ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes()));
    }

    public void testAdaptiveFormulaAllComponents() {
        int numSegments = randomIntBetween(1, 100);
        int totalFields = randomIntBetween(1, 100);
        long liveDocsBytes = randomLongBetween(0, 100_000);
        long pointsBytes = randomLongBetween(0, 100_000);
        ShardHeapEstimator estimator = adaptiveEstimator(0.0, 0L);
        var m = metrics(
            0,
            numSegments,
            totalFields,
            0,
            liveDocsBytes,
            pointsBytes,
            UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
            MetricQuality.EXACT
        );
        long expected = ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + (long) numSegments * ADAPTIVE_SEGMENT_MEMORY_OVERHEAD.getBytes()
            + (long) totalFields * ADAPTIVE_FIELD_MEMORY_OVERHEAD.getBytes() + liveDocsBytes + pointsBytes;
        assertThat(estimator.computeShardHeapUsage(m), equalTo(expected));
    }

    public void testAdaptiveExtraOverheadRatioApplied() {
        int numSegments = 2;
        int totalFields = 10;
        double extraRatio = 0.25;
        ShardHeapEstimator estimator = adaptiveEstimator(extraRatio, 0L);
        var m = metrics(0, numSegments, totalFields, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        long baseEstimate = ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + (long) numSegments * ADAPTIVE_SEGMENT_MEMORY_OVERHEAD.getBytes()
            + (long) totalFields * ADAPTIVE_FIELD_MEMORY_OVERHEAD.getBytes();
        long expected = baseEstimate + (long) (baseEstimate * extraRatio);
        assertThat(estimator.computeShardHeapUsage(m), equalTo(expected));
    }

    public void testAdaptiveMinThresholdEnforced() {
        // choose a min threshold larger than the adaptive estimate for zero-segment, zero-field shard
        long minThreshold = ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() * 10;
        ShardHeapEstimator estimator = adaptiveEstimator(0.0, minThreshold);
        var m = metrics(0, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        assertThat(estimator.computeShardHeapUsage(m), equalTo(minThreshold));
    }

    public void testAdaptiveEstimateNotBelowMinThreshold() {
        long minThreshold = randomLongBetween(1, 1_000_000_000);
        ShardHeapEstimator estimator = adaptiveEstimator(0.0, minThreshold);
        var m = metrics(
            0,
            randomIntBetween(0, 5),
            randomIntBetween(0, 5),
            0,
            0,
            0,
            UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
            MetricQuality.EXACT
        );
        assertThat(estimator.computeShardHeapUsage(m), greaterThanOrEqualTo(minThreshold));
    }

    // --- computeIndexHeapUsage ---

    public void testComputeIndexHeapUsageReturnsMappingSize() {
        long mappingSize = randomLongBetween(0, 10_000_000);
        ShardHeapEstimator estimator = adaptiveEstimator(0.0, 0L);
        var m = metrics(mappingSize, 1, 5, 500, 100, 50, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        assertThat(estimator.computeIndexHeapUsage(m), equalTo(mappingSize));
    }

    // --- aggregateShardMetrics ---

    public void testAggregateShardMetricsEmpty() {
        ShardHeapEstimator estimator = adaptiveEstimator(0.0, 0L);
        var result = estimator.aggregateShardMetrics(Map.of(), (id, m) -> {});
        assertThat(result.totalShardHeapInBytes(), equalTo(0L));
        assertThat(result.maxShardHeapInBytes(), equalTo(0L));
        assertThat(result.mappingSizeInBytes(), equalTo(0L));
        assertThat(result.metricQuality(), equalTo(MetricQuality.EXACT));
    }

    public void testAggregateShardMetricsSumsTotals() {
        ShardHeapEstimator estimator = fixedEstimator(ByteSizeValue.ofBytes(1000));
        long mapping1 = 200, mapping2 = 300;
        var m1 = metrics(mapping1, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        var m2 = metrics(mapping2, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        ShardId id1 = new ShardId(new Index("idx1", "uuid1"), 0);
        ShardId id2 = new ShardId(new Index("idx2", "uuid2"), 0);

        var result = estimator.aggregateShardMetrics(Map.of(id1, m1, id2, m2), (id, m) -> {});
        assertThat(result.totalShardHeapInBytes(), equalTo(2000L));
        assertThat(result.maxShardHeapInBytes(), equalTo(1000L));
        assertThat(result.mappingSizeInBytes(), equalTo(mapping1 + mapping2));
        assertThat(result.metricQuality(), equalTo(MetricQuality.EXACT));
    }

    public void testAggregateShardMetricsMaxShardHeap() {
        // use adaptive so shard heap varies with segment count
        ShardHeapEstimator estimator = adaptiveEstimator(0.0, 0L);
        var small = metrics(0, 1, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        var large = metrics(0, 100, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        ShardId id1 = new ShardId(new Index("idx", "uuid"), 0);
        ShardId id2 = new ShardId(new Index("idx", "uuid"), 1);

        var result = estimator.aggregateShardMetrics(Map.of(id1, small, id2, large), (id, m) -> {});
        assertThat(result.maxShardHeapInBytes(), equalTo(estimator.computeShardHeapUsage(large)));
    }

    public void testAggregateShardMetricsPropagatesNonExactQuality() {
        ShardHeapEstimator estimator = fixedEstimator(ByteSizeValue.ofBytes(500));
        var exact = metrics(0, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        var minimum = metrics(0, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.MINIMUM);
        ShardId id1 = new ShardId(new Index("idx1", "uuid1"), 0);
        ShardId id2 = new ShardId(new Index("idx2", "uuid2"), 0);

        var result = estimator.aggregateShardMetrics(Map.of(id1, exact, id2, minimum), (id, m) -> {});
        assertThat(result.metricQuality(), equalTo(MetricQuality.MINIMUM));
    }

    public void testAggregateShardMetricsMissingQualityPropagated() {
        ShardHeapEstimator estimator = fixedEstimator(ByteSizeValue.ofBytes(500));
        var exact = metrics(0, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        var missing = metrics(0, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.MISSING);
        ShardId id1 = new ShardId(new Index("idx1", "uuid1"), 0);
        ShardId id2 = new ShardId(new Index("idx2", "uuid2"), 0);

        var result = estimator.aggregateShardMetrics(Map.of(id1, exact, id2, missing), (id, m) -> {});
        assertThat(result.metricQuality(), equalTo(MetricQuality.MISSING));
    }

    public void testAggregateShardMetricsVisitorCalledForEachShard() {
        ShardHeapEstimator estimator = fixedEstimator(ByteSizeValue.ofBytes(500));
        var m1 = metrics(0, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        var m2 = metrics(0, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        ShardId id1 = new ShardId(new Index("idx1", "uuid1"), 0);
        ShardId id2 = new ShardId(new Index("idx2", "uuid2"), 0);
        Map<ShardId, StatelessMemoryMetricsService.ShardMemoryMetrics> input = Map.of(id1, m1, id2, m2);

        Set<ShardId> visited = new HashSet<>();
        estimator.aggregateShardMetrics(input, (id, m) -> visited.add(id));
        assertThat(visited, equalTo(input.keySet()));
    }

    public void testAggregateShardMetricsMappingSizeIsSumOfIndexHeapUsages() {
        ShardHeapEstimator estimator = adaptiveEstimator(0.0, 0L);
        long mapping1 = randomLongBetween(100, 10_000);
        long mapping2 = randomLongBetween(100, 10_000);
        var m1 = metrics(mapping1, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        var m2 = metrics(mapping2, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        ShardId id1 = new ShardId(new Index("idx1", "uuid1"), 0);
        ShardId id2 = new ShardId(new Index("idx2", "uuid2"), 0);

        var result = estimator.aggregateShardMetrics(Map.of(id1, m1, id2, m2), (id, m) -> {});
        assertThat(result.mappingSizeInBytes(), equalTo(mapping1 + mapping2));
    }

    // --- getEffectiveShardPostingsInBytes ---

    public void testAggregateShardMetricsMissingBeatsMinimumRegardlessOfOrder() {
        ShardHeapEstimator estimator = fixedEstimator(ByteSizeValue.ofBytes(500));
        var minimum = metrics(0, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.MINIMUM);
        var missing = metrics(0, 0, 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.MISSING);
        ShardId id1 = new ShardId(new Index("idx1", "uuid1"), 0);
        ShardId id2 = new ShardId(new Index("idx2", "uuid2"), 0);

        // MISSING then MINIMUM
        var missingFirst = new LinkedHashMap<ShardId, StatelessMemoryMetricsService.ShardMemoryMetrics>();
        missingFirst.put(id1, missing);
        missingFirst.put(id2, minimum);
        assertThat(estimator.aggregateShardMetrics(missingFirst, (id, m) -> {}).metricQuality(), equalTo(MetricQuality.MISSING));

        // MINIMUM then MISSING
        var minimumFirst = new LinkedHashMap<ShardId, StatelessMemoryMetricsService.ShardMemoryMetrics>();
        minimumFirst.put(id1, minimum);
        minimumFirst.put(id2, missing);
        assertThat(estimator.aggregateShardMetrics(minimumFirst, (id, m) -> {}).metricQuality(), equalTo(MetricQuality.MISSING));
    }

    public void testEffectivePostingsZeroWhenPostingsIncludedInEstimate() {
        // includePostingsInEstimate=true: postings are already folded into computeShardHeapUsage, so effective postings must be 0
        long postings = randomLongBetween(1, 1_000_000);
        ShardHeapEstimator estimator = new ShardHeapEstimator(ByteSizeValue.ZERO, 0.0, 0L, false, true);
        var m = metrics(0, 0, 0, postings, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        assertThat(estimator.getEffectiveShardPostingsInBytes(m), equalTo(0L));
    }

    public void testEffectivePostingsReturnedWhenSelfReportedDisabled() {
        // includePostingsInEstimate=false, selfReported=false: postings tracked separately, adaptive estimate used for shard
        long postings = randomLongBetween(1, 1_000_000);
        long selfReportedOverhead = randomLongBetween(1, 10_000_000); // defined, but self-reported is disabled
        ShardHeapEstimator estimator = new ShardHeapEstimator(ByteSizeValue.ZERO, 0.0, 0L, false, false);
        var m = metrics(0, 0, 0, postings, 0, 0, selfReportedOverhead, MetricQuality.EXACT);
        assertThat(estimator.getEffectiveShardPostingsInBytes(m), equalTo(postings));
    }

    public void testEffectivePostingsReturnedWhenSelfReportedEnabledButUndefined() {
        // includePostingsInEstimate=false, selfReported=true but no value reported: adaptive estimate used, postings tracked separately
        long postings = randomLongBetween(1, 1_000_000);
        ShardHeapEstimator estimator = new ShardHeapEstimator(ByteSizeValue.ZERO, 0.0, 0L, true, false);
        var m = metrics(0, 0, 0, postings, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, MetricQuality.EXACT);
        assertThat(estimator.getEffectiveShardPostingsInBytes(m), equalTo(postings));
    }

    public void testEffectivePostingsZeroWhenSelfReportedAvailable() {
        // includePostingsInEstimate=false, selfReported=true and defined: self-reported overhead already covers postings, return 0
        long postings = randomLongBetween(1, 1_000_000);
        long selfReportedOverhead = randomLongBetween(1, 10_000_000);
        ShardHeapEstimator estimator = new ShardHeapEstimator(ByteSizeValue.ZERO, 0.0, 0L, true, false);
        var m = metrics(0, 0, 0, postings, 0, 0, selfReportedOverhead, MetricQuality.EXACT);
        assertThat(estimator.getEffectiveShardPostingsInBytes(m), equalTo(0L));
    }
}
