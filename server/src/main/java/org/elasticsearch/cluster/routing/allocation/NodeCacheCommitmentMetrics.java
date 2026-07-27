/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;

/**
 * Publishes per-node cache commitment metrics derived from {@link ClusterInfo#getNodeCacheSizeAndCommitments()}.
 * Each metric is expressed as a fraction of the node's total cache size: 1.0 means exactly committed, 1.5 means
 * 50% over-committed.
 * <p>
 * Two gauges are emitted per node for which data is available:
 * <ul>
 *     <li>boosted commitment only</li>
 *     <li>total commitment (boosted + unboosted)</li>
 * </ul>
 * <p>
 * Each gauge holds its own {@link ClusterInfo} reference that is consumed (cleared) when APM polls, so each
 * {@link ClusterInfo} drives at most one round of reported metrics per gauge.
 */
public class NodeCacheCommitmentMetrics {

    public static final String BOOSTED_CACHE_COMMITMENT_METRIC_NAME = "es.allocator.cache_commitments.boosted.current";
    public static final String TOTAL_CACHE_COMMITMENT_METRIC_NAME = "es.allocator.cache_commitments.total.current";

    private final ClusterService clusterService;
    private final AtomicReference<ClusterInfo> latestClusterInfo = new AtomicReference<>();
    private final AtomicReference<Collection<DoubleWithAttributes>> boostedCommitmentMetrics = new AtomicReference<>();
    private final AtomicReference<Collection<DoubleWithAttributes>> totalCommitmentMetrics = new AtomicReference<>();

    public NodeCacheCommitmentMetrics(MeterRegistry meterRegistry, ClusterService clusterService) {
        this.clusterService = clusterService;
        meterRegistry.registerDoublesGauge(
            BOOSTED_CACHE_COMMITMENT_METRIC_NAME,
            "Boosted cache commitment as a fraction of total cache size per node",
            "1",
            this::getBoostedCommitmentMetrics
        );
        meterRegistry.registerDoublesGauge(
            TOTAL_CACHE_COMMITMENT_METRIC_NAME,
            "Total cache commitment (boosted + unboosted) as a fraction of total cache size per node",
            "1",
            this::getTotalCommitmentMetrics
        );
    }

    public void onNewInfo(ClusterInfo clusterInfo) {
        latestClusterInfo.set(clusterInfo);
    }

    /// Compute both sets of metrics from the latest [ClusterInfo] if we haven't already computed them since the last poll.
    ///
    /// This ensures that for any poll the two sets of metrics are consistent
    private void computeMetricsFromClusterInfo() {
        if (boostedCommitmentMetrics.get() == null && totalCommitmentMetrics.get() == null) {
            final var clusterInfo = latestClusterInfo.getAndSet(null);
            if (clusterInfo != null) {
                final var attributesCache = new HashMap<DiscoveryNode, Map<String, Object>>();
                boostedCommitmentMetrics.set(
                    computeMetrics(clusterInfo, NodeCacheSizeAndCommitments::boostedCacheCommitmentInBytes, attributesCache)
                );
                totalCommitmentMetrics.set(
                    computeMetrics(clusterInfo, NodeCacheSizeAndCommitments::totalCacheCommitmentInBytes, attributesCache)
                );
            }
        }
    }

    private static Map<String, Object> getAttributesForNode(DiscoveryNode node) {
        return Map.of("es_node_id", node.getId(), "es_node_name", node.getName());
    }

    // visible for testing
    final Collection<DoubleWithAttributes> getBoostedCommitmentMetrics() {
        computeMetricsFromClusterInfo();
        final var metrics = boostedCommitmentMetrics.getAndSet(null);
        return metrics != null ? metrics : List.of();
    }

    // visible for testing
    final Collection<DoubleWithAttributes> getTotalCommitmentMetrics() {
        computeMetricsFromClusterInfo();
        final var metrics = totalCommitmentMetrics.getAndSet(null);
        return metrics != null ? metrics : List.of();
    }

    private Collection<DoubleWithAttributes> computeMetrics(
        ClusterInfo clusterInfo,
        ToLongFunction<NodeCacheSizeAndCommitments> commitmentFunction,
        Map<DiscoveryNode, Map<String, Object>> attributesCache
    ) {
        if (clusterService.lifecycleState() != Lifecycle.State.STARTED) {
            return List.of();
        }

        var nodeCacheSizeAndCommitments = clusterInfo.getNodeCacheSizeAndCommitments();
        if (nodeCacheSizeAndCommitments.isEmpty()) {
            return List.of();
        }

        var clusterState = clusterService.state();
        var metrics = new ArrayList<DoubleWithAttributes>(nodeCacheSizeAndCommitments.size());

        for (var entry : nodeCacheSizeAndCommitments.entrySet()) {
            final var nodeCommitments = entry.getValue();
            final long nodeCacheSizeInBytes = nodeCommitments.cacheSizeInBytes();
            if (nodeCacheSizeInBytes <= 0) {
                continue;
            }

            final var discoveryNode = clusterState.nodes().get(entry.getKey());
            if (discoveryNode == null) {
                continue;
            }

            double value = commitmentFunction.applyAsLong(nodeCommitments) / (double) nodeCacheSizeInBytes;
            metrics.add(
                new DoubleWithAttributes(
                    value,
                    attributesCache.computeIfAbsent(discoveryNode, NodeCacheCommitmentMetrics::getAttributesForNode)
                )
            );
        }

        return metrics;
    }
}
