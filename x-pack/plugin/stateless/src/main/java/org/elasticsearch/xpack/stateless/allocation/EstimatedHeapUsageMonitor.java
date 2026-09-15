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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class EstimatedHeapUsageMonitor {

    /**
     * Configures the resource usage and watermarks observed by an {@link EstimatedHeapUsageMonitor}.
     *
     * @param description human-readable resource description used in log messages and reroute reasons
     * @param thresholdEnabledSetting setting that enables monitoring
     * @param lowWatermarkSetting low watermark setting
     * @param highWatermarkEnabledSetting setting that enables the high watermark
     * @param highWatermarkSetting high watermark setting
     * @param nodeUsagePercentages extracts the resource usage percentage for each node from cluster information and state
     */
    public record Configuration(
        String description,
        Setting<Boolean> thresholdEnabledSetting,
        Setting<RatioValue> lowWatermarkSetting,
        Setting<Boolean> highWatermarkEnabledSetting,
        Setting<RatioValue> highWatermarkSetting,
        BiFunction<ClusterInfo, ClusterState, Map<String, Double>> nodeUsagePercentages
    ) {}

    private static final Logger logger = LogManager.getLogger(EstimatedHeapUsageMonitor.class);

    private final Supplier<ClusterState> clusterStateSupplier;
    private final RerouteService rerouteService;
    private final String description;
    private final BiFunction<ClusterInfo, ClusterState, Map<String, Double>> nodeUsagePercentages;
    private volatile boolean thresholdEnabled;
    private volatile boolean highWatermarkEnabled;
    private volatile RatioValue lowWatermark;
    private volatile RatioValue highWatermark;
    private final AtomicReference<Set<String>> lastKnownNodeIdsExceedingLowWatermark = new AtomicReference<>(Set.of());
    private final AtomicReference<Set<String>> lastKnownNodeIdsExceedingHighWatermark = new AtomicReference<>(Set.of());

    /**
     * Creates a monitor for the resource described by {@code configuration}.
     */
    public EstimatedHeapUsageMonitor(
        ClusterSettings clusterSettings,
        Supplier<ClusterState> clusterStateSupplier,
        RerouteService rerouteService,
        Configuration configuration
    ) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.rerouteService = rerouteService;
        this.description = configuration.description();
        this.nodeUsagePercentages = configuration.nodeUsagePercentages();
        clusterSettings.initializeAndWatch(configuration.thresholdEnabledSetting(), newValue -> this.thresholdEnabled = newValue);
        clusterSettings.initializeAndWatch(configuration.lowWatermarkSetting(), newValue -> this.lowWatermark = newValue);
        clusterSettings.initializeAndWatch(configuration.highWatermarkEnabledSetting(), newValue -> this.highWatermarkEnabled = newValue);
        clusterSettings.initializeAndWatch(configuration.highWatermarkSetting(), newValue -> this.highWatermark = newValue);
    }

    public void onNewInfo(ClusterInfo clusterInfo) {
        final ClusterState clusterState = clusterStateSupplier.get();
        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("skipping monitor as the cluster state is not recovered yet");
            return;
        }

        if (thresholdEnabled == false) {
            logger.debug("skipping monitor as the {} usage threshold is disabled", description);
            return;
        }

        final Map<String, Double> usagesByNode = nodeUsagePercentages.apply(clusterInfo, clusterState);
        final var nodeIdsExceedingLowWatermark = usagesByNode.entrySet()
            .stream()
            .filter(entry -> entry.getValue() > lowWatermark.getAsPercent())
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableSet());

        final var previousNodeIds = lastKnownNodeIdsExceedingLowWatermark.getAndSet(nodeIdsExceedingLowWatermark);
        if (nodeIdsExceedingLowWatermark.containsAll(previousNodeIds) == false) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    Strings.format(
                        "%s usages dropped below the low watermark [%.2f] for nodes %s, triggering reroute",
                        description,
                        lowWatermark.getAsPercent(),
                        Sets.difference(previousNodeIds, nodeIdsExceedingLowWatermark)
                    )
                );
            }
            final String reason = description + " usages drop below low watermark";
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
            final var nodeIdsExceedingHighWatermark = usagesByNode.entrySet()
                .stream()
                .filter(entry -> entry.getValue() > highWatermark.getAsPercent())
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());

            final var previousHighWatermarkNodeIds = lastKnownNodeIdsExceedingHighWatermark.getAndSet(nodeIdsExceedingHighWatermark);
            if (previousHighWatermarkNodeIds.containsAll(nodeIdsExceedingHighWatermark) == false) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        Strings.format(
                            "%s usages exceeded the high watermark [%.2f] for nodes %s, triggering reroute",
                            description,
                            highWatermark.getAsPercent(),
                            Sets.difference(nodeIdsExceedingHighWatermark, previousHighWatermarkNodeIds)
                        )
                    );
                }
                final String reason = description + " usages exceeded high watermark";
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
