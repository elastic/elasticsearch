/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.xpack.stateless.allocation.EstimatedHeapUsageAllocationDecider;

/// The estimated-heap intervention settings, shared by [EstimatedHeapUsageAllocationDecider] (master) and the data-node
/// recovery gate (`EstimatedHeapUsageRecoveryGate`) so applicability, enablement, and watermark semantics cannot drift between
/// the two. [StatelessPlugin] creates a single instance and injects it into consumers.
public final class EstimatedHeapSettings {

    private volatile boolean enabled;
    private volatile RatioValue lowWatermark;
    private volatile boolean highWatermarkEnabled;
    private volatile RatioValue highWatermark;
    private volatile ByteSizeValue minimumHeapSizeForEnablement;

    public EstimatedHeapSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
            value -> enabled = value
        );
        clusterSettings.initializeAndWatch(
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
            value -> lowWatermark = value
        );
        clusterSettings.initializeAndWatch(
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED,
            value -> highWatermarkEnabled = value
        );
        clusterSettings.initializeAndWatch(
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK,
            value -> highWatermark = value
        );
        clusterSettings.initializeAndWatch(
            EstimatedHeapUsageAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT,
            value -> minimumHeapSizeForEnablement = value
        );
    }

    /// Whether heap estimation and intervention apply to the given node: index nodes only.
    public static boolean appliesToNode(DiscoveryNode node) {
        return node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE);
    }

    /// Variant of [#appliesToNode(DiscoveryNode)] for wiring-time checks (e.g. creating the recovery gate), which run before
    /// the local [DiscoveryNode] exists.
    public static boolean appliesToNode(Settings settings) {
        return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE);
    }

    public boolean enabled() {
        return enabled;
    }

    /// Nodes whose max heap is below [#minimumHeapSizeForEnablement()] are exempt from heap-based intervention.
    public boolean belowMinimumHeapForEnablement(long nodeMaxHeapBytes) {
        return nodeMaxHeapBytes < minimumHeapSizeForEnablement.getBytes();
    }

    public ByteSizeValue minimumHeapSizeForEnablement() {
        return minimumHeapSizeForEnablement;
    }

    public double lowWatermarkPercent() {
        return lowWatermark.getAsPercent();
    }

    public boolean exceedsLowWatermark(double heapUsedPercent) {
        return heapUsedPercent > lowWatermarkPercent();
    }

    public boolean highWatermarkEnabled() {
        return highWatermarkEnabled;
    }

    public double highWatermarkPercent() {
        return highWatermark.getAsPercent();
    }

    public boolean exceedsHighWatermark(double heapUsedPercent) {
        return heapUsedPercent > highWatermarkPercent();
    }
}
