/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.IntCountsHistogram;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;

/**
 * Publishes metrics about the distribution of shard write loads on each node in the cluster
 */
public class ShardWriteLoadDistributionMetrics {

    private static final Logger logger = LogManager.getLogger(ShardWriteLoadDistributionMetrics.class);
    private static final String WRITE_LOAD_DISTRIBUTION_METRIC_NAME = "es.allocator.shard_write_load.distribution.p%s.current";
    public static final String WRITE_LOAD_PRIORITISATION_THRESHOLD_METRIC_NAME =
        "es.allocator.shard_write_load.prioritisation_threshold.current";
    public static final String WRITE_LOAD_PRIORITISATION_THRESHOLD_PERCENTILE_RANK_METRIC_NAME =
        "es.allocator.shard_write_load.prioritisation_threshold.shard_count_exceeding.current";
    public static final String WRITE_LOAD_SUM_METRIC_NAME = "es.allocator.shard_write_load.sum.current";
    public static final Setting<Boolean> SHARD_WRITE_LOAD_METRICS_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.write_load_decider.shard_write_load_metrics.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final DoubleHistogram shardWeightHistogram;
    private final int[] percentiles;
    private final ClusterService clusterService;
    private final AtomicReferenceArray<List<DoubleWithAttributes>> lastWriteLoadDistributionValues;
    private final AtomicReference<List<DoubleWithAttributes>> lastWriteLoadPrioritisationThresholdValues = new AtomicReference<>(List.of());
    private final AtomicReference<List<LongWithAttributes>> lastShardCountExceedingPrioritisationThresholdValues = new AtomicReference<>(
        List.of()
    );
    private final AtomicReference<List<DoubleWithAttributes>> lastWriteLoadSumValues = new AtomicReference<>(List.of());
    private volatile boolean metricsEnabled = false;

    public ShardWriteLoadDistributionMetrics(MeterRegistry meterRegistry, ClusterService clusterService) {
        // 2 significant digits means error < 1% of any value in the range
        this(meterRegistry, clusterService, 2, 50, 90, 95, 99, 100);
    }

    public ShardWriteLoadDistributionMetrics(
        MeterRegistry meterRegistry,
        ClusterService clusterService,
        int numberOfSignificantDigits,
        int... percentiles
    ) {
        this.clusterService = clusterService;
        this.clusterService.getClusterSettings()
            .initializeAndWatch(SHARD_WRITE_LOAD_METRICS_ENABLED_SETTING, value -> this.metricsEnabled = value);

        this.shardWeightHistogram = new DoubleHistogram(numberOfSignificantDigits, IntCountsHistogram.class);
        this.percentiles = percentiles;
        this.lastWriteLoadDistributionValues = new AtomicReferenceArray<>(percentiles.length);
        IntStream.range(0, percentiles.length).forEach(percentileIndex -> {
            lastWriteLoadDistributionValues.set(percentileIndex, List.of());
            meterRegistry.registerDoublesGauge(
                shardWriteLoadDistributionMetricName(percentiles[percentileIndex]),
                percentiles[percentileIndex] + "th percentile of shard write-load values, broken down by node",
                "write load",
                () -> this.getWriteLoadDistributionMetrics(percentileIndex)
            );
        });
        meterRegistry.registerDoublesGauge(
            WRITE_LOAD_PRIORITISATION_THRESHOLD_METRIC_NAME,
            "The threshold over which shards will be prioritised for movement when hot-spotting, per node",
            "write load",
            this::getWriteLoadPrioritisationThresholdMetrics
        );
        meterRegistry.registerLongsGauge(
            WRITE_LOAD_PRIORITISATION_THRESHOLD_PERCENTILE_RANK_METRIC_NAME,
            "The number of shards whose write-load exceeds the prioritisation threshold, per node",
            "unit",
            this::getWriteLoadPrioritisationThresholdPercentileRankMetrics
        );
        meterRegistry.registerDoublesGauge(
            WRITE_LOAD_SUM_METRIC_NAME,
            "The sum of the shard write-loads for the shards allocated to each node",
            "write load",
            this::getWriteLoadSumMetrics
        );
    }

    public void onNewInfo(ClusterInfo clusterInfo) {
        // We need a cluster state and shard write loads to compute these metrics
        if (metricsEnabled == false
            || clusterService.lifecycleState() != Lifecycle.State.STARTED
            || clusterInfo.getShardWriteLoads().isEmpty()
            || thereAreUncollectedMetrics()) {
            return;
        }

        final var clusterState = clusterService.state();
        final var shardWriteLoads = clusterInfo.getShardWriteLoads();
        final var ingestNodeCount = (int) clusterState.nodes().stream().filter(this::isIndexingNode).count();
        final var writeLoadDistributionValues = createPercentileArrays(percentiles.length, ingestNodeCount);
        final var writeLoadPrioritisationThresholdValues = new ArrayList<DoubleWithAttributes>(ingestNodeCount);
        final var shardCountsExceedingPrioritisationThresholdValues = new ArrayList<LongWithAttributes>(ingestNodeCount);
        final var shardWriteLoadSumValues = new ArrayList<DoubleWithAttributes>(ingestNodeCount);
        for (RoutingNode routingNode : clusterState.getRoutingNodes()) {
            final var node = routingNode.node();
            if (node == null || isIndexingNode(node) == false) {
                continue;
            }

            double maxShardWriteLoad = Double.NEGATIVE_INFINITY;
            double totalShardWriteLoad = 0.0;

            shardWeightHistogram.reset();
            try {
                for (ShardRouting shardRouting : routingNode) {
                    final double writeLoad = shardWriteLoads.getOrDefault(shardRouting.shardId(), 0.0);
                    /*
                     * Shard write-loads originate from an org.elasticsearch.common.metrics.ExponentiallyWeightedMovingRate,
                     * they sometimes end up being calculated as very small (e.g. 3.3123178228374412E-21). These values
                     * don't play nice with the HdrHistogram because it works best when there is a relatively small difference
                     * in the scale of the values inserted into it.
                     * They're also not very interesting, so we round them down to zero before adding them to the histogram.
                     */
                    shardWeightHistogram.recordValue(roundTinyValuesToZero(writeLoad));
                    maxShardWriteLoad = Math.max(maxShardWriteLoad, writeLoad);
                    totalShardWriteLoad += writeLoad;
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                // This shouldn't happen because our histogram should be auto-resizing, but just in case
                final var message = "Failed to record shard write load distribution metrics for node "
                    + node.getName()
                    + " ("
                    + e.getMessage()
                    + ")";
                assert false : message;
                logger.error(message, e);
                continue;
            }

            final var nodeAttrs = getAttributesForNode(node);

            /*
             * Only publish distribution and prioritization threshold metrics if the node contains at least one shard
             */
            if (Double.isFinite(maxShardWriteLoad)) {
                for (int i = 0; i < percentiles.length; i++) {
                    writeLoadDistributionValues[i].add(
                        new DoubleWithAttributes(shardWeightHistogram.getValueAtPercentile(percentiles[i]), nodeAttrs)
                    );
                }

                final double prioritisationThreshold = BalancedShardsAllocator.Balancer.PrioritiseByShardWriteLoadComparator.THRESHOLD_RATIO
                    * maxShardWriteLoad;
                writeLoadPrioritisationThresholdValues.add(new DoubleWithAttributes(prioritisationThreshold, nodeAttrs));

                final long shardsExceedingThreshold = (long) shardWeightHistogram.getCountBetweenValues(
                    prioritisationThreshold,
                    Double.MAX_VALUE
                );
                shardCountsExceedingPrioritisationThresholdValues.add(new LongWithAttributes(shardsExceedingThreshold, nodeAttrs));
            }
            shardWriteLoadSumValues.add(new DoubleWithAttributes(totalShardWriteLoad, nodeAttrs));
        }

        for (int i = 0; i < percentiles.length; i++) {
            lastWriteLoadDistributionValues.set(i, writeLoadDistributionValues[i]);
        }
        lastWriteLoadPrioritisationThresholdValues.set(writeLoadPrioritisationThresholdValues);
        lastShardCountExceedingPrioritisationThresholdValues.set(shardCountsExceedingPrioritisationThresholdValues);
        lastWriteLoadSumValues.set(shardWriteLoadSumValues);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private List<DoubleWithAttributes>[] createPercentileArrays(int percentileCount, int ingestNodeCount) {
        List<DoubleWithAttributes>[] lists = (List<DoubleWithAttributes>[]) new List[percentileCount];
        for (int i = 0; i < percentileCount; i++) {
            lists[i] = new ArrayList<>(ingestNodeCount);
        }
        return lists;
    }

    /**
     * Get the metric name for the shard write load distribution metric for the
     * specified percentile.
     *
     * @param percentile The desired percentile
     * @return The metric name
     */
    public static String shardWriteLoadDistributionMetricName(int percentile) {
        return Strings.format(WRITE_LOAD_DISTRIBUTION_METRIC_NAME, percentile);
    }

    /**
     * We only calculate metrics for indexing nodes.
     * <p>
     * Note this means it only works for stateless currently
     */
    private boolean isIndexingNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE);
    }

    /**
     * We receive ClusterInfo more often than we publish metrics, so don't recalculate
     * the metrics if the last ones haven't been published yet.
     * <p>
     * This means the metrics can lag by ~30s, but it avoids calculating metrics that are
     * discarded. An alternative would be to calculate the metrics on the metrics thread,
     * but it could be quite expensive for larger clusters.
     *
     * @return true if there are uncollected metrics, false otherwise.
     */
    private boolean thereAreUncollectedMetrics() {
        for (int i = 0; i < percentiles.length; i++) {
            if (lastWriteLoadDistributionValues.get(i).isEmpty() == false) {
                return true;
            }
        }
        return lastWriteLoadPrioritisationThresholdValues.get().isEmpty() == false
            || lastShardCountExceedingPrioritisationThresholdValues.get().isEmpty() == false
            || lastWriteLoadSumValues.get().isEmpty() == false;
    }

    private double roundTinyValuesToZero(double value) {
        assert value >= 0.0 : "We got a negative write load?! " + value;
        return value < 0.01 ? 0.0 : value;
    }

    private Map<String, Object> getAttributesForNode(DiscoveryNode node) {
        return Map.of("es_node_id", node.getId(), "es_node_name", node.getName());
    }

    private Collection<DoubleWithAttributes> getWriteLoadDistributionMetrics(int index) {
        return lastWriteLoadDistributionValues.getAndSet(index, List.of());
    }

    private Collection<DoubleWithAttributes> getWriteLoadPrioritisationThresholdMetrics() {
        return lastWriteLoadPrioritisationThresholdValues.getAndSet(List.of());
    }

    private Collection<LongWithAttributes> getWriteLoadPrioritisationThresholdPercentileRankMetrics() {
        return lastShardCountExceedingPrioritisationThresholdValues.getAndSet(List.of());
    }

    private Collection<DoubleWithAttributes> getWriteLoadSumMetrics() {
        return lastWriteLoadSumValues.getAndSet(List.of());
    }
}
