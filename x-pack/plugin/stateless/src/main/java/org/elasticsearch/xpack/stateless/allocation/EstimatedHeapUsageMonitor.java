/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class EstimatedHeapUsageMonitor {

    private static final Logger logger = LogManager.getLogger(EstimatedHeapUsageMonitor.class);

    private final Supplier<ClusterState> clusterStateSupplier;
    private final RerouteService rerouteService;
    private volatile boolean thresholdEnabled;
    private volatile boolean highWatermarkEnabled;
    private volatile RatioValue estimatedHeapLowWatermark;
    private volatile RatioValue estimatedHeapHighWatermark;
    private final AtomicReference<Set<String>> lastKnownNodeIdsExceedingLowWatermark = new AtomicReference<>(Set.of());
    private final AtomicReference<Set<String>> lastKnownNodeIdsExceedingHighWatermark = new AtomicReference<>(Set.of());

    public EstimatedHeapUsageMonitor(
        ClusterSettings clusterSettings,
        Supplier<ClusterState> clusterStateSupplier,
        RerouteService rerouteService
    ) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.rerouteService = rerouteService;
        clusterSettings.initializeAndWatch(
            InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
            newValue -> this.thresholdEnabled = newValue
        );
        clusterSettings.initializeAndWatch(
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
            newValue -> this.estimatedHeapLowWatermark = newValue
        );
        clusterSettings.initializeAndWatch(
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED,
            newValue -> this.highWatermarkEnabled = newValue
        );
        clusterSettings.initializeAndWatch(
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK,
            newValue -> this.estimatedHeapHighWatermark = newValue
        );
    }

    public void onNewInfo(ClusterInfo clusterInfo) {
        if (clusterStateSupplier.get().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("skipping monitor as the cluster state is not recovered yet");
            return;
        }

        if (thresholdEnabled == false) {
            logger.debug("skipping monitor as the estimated heap usage threshold is disabled");
            return;
        }

        final var nodeIdsExceedingLowWatermark = clusterInfo.getEstimatedHeapUsages()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().estimatedUsageAsPercentage() > estimatedHeapLowWatermark.getAsPercent())
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableSet());

        final var previousNodeIds = lastKnownNodeIdsExceedingLowWatermark.getAndSet(nodeIdsExceedingLowWatermark);
        if (nodeIdsExceedingLowWatermark.containsAll(previousNodeIds) == false) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    Strings.format(
                        "estimated heap usages dropped below the low watermark [%.2f] for nodes %s, triggering reroute",
                        estimatedHeapLowWatermark.getAsPercent(),
                        Sets.difference(previousNodeIds, nodeIdsExceedingLowWatermark)
                    )
                );
            }
            final String reason = "estimated heap usages drop below low watermark";
            rerouteService.reroute(
                reason,
                Priority.NORMAL,
                ActionListener.wrap(
                    ignored -> logger.trace("{} reroute successful", reason),
                    e -> logger.debug(() -> Strings.format("reroute failed, reason: %s", reason), e)
                )
            );
        }

        if (highWatermarkEnabled) {
            final var nodeIdsExceedingHighWatermark = clusterInfo.getEstimatedHeapUsages()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().estimatedUsageAsPercentage() > estimatedHeapHighWatermark.getAsPercent())
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());

            final var previousHighWatermarkNodeIds = lastKnownNodeIdsExceedingHighWatermark.getAndSet(nodeIdsExceedingHighWatermark);
            if (previousHighWatermarkNodeIds.containsAll(nodeIdsExceedingHighWatermark) == false) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        Strings.format(
                            "estimated heap usages exceeded the high watermark [%.2f] for nodes %s, triggering reroute",
                            estimatedHeapHighWatermark.getAsPercent(),
                            Sets.difference(nodeIdsExceedingHighWatermark, previousHighWatermarkNodeIds)
                        )
                    );
                }
                final String reason = "estimated heap usages exceeded high watermark";
                rerouteService.reroute(
                    reason,
                    Priority.NORMAL,
                    ActionListener.wrap(
                        ignored -> logger.trace("{} reroute successful", reason),
                        e -> logger.debug(() -> Strings.format("reroute failed, reason: %s", reason), e)
                    )
                );
            }
        } else {
            lastKnownNodeIdsExceedingHighWatermark.set(Set.of());
        }
    }
}
