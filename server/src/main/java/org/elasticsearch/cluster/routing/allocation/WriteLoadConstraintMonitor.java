/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.gateway.GatewayService;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Monitors the node-level write thread pool usage across the cluster and initiates (coming soon) a rebalancing round (via
 * {@link RerouteService#reroute}) whenever a node crosses the node-level write load thresholds.
 *
 * TODO (ES-11992): implement
 */
public class WriteLoadConstraintMonitor {
    private static final Logger logger = LogManager.getLogger(WriteLoadConstraintMonitor.class);
    private final WriteLoadConstraintSettings writeLoadConstraintSettings;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final LongSupplier currentTimeMillisSupplier;
    private final RerouteService rerouteService;

    public WriteLoadConstraintMonitor(
        ClusterSettings clusterSettings,
        LongSupplier currentTimeMillisSupplier,
        Supplier<ClusterState> clusterStateSupplier,
        RerouteService rerouteService
    ) {
        this.writeLoadConstraintSettings = new WriteLoadConstraintSettings(clusterSettings);
        this.clusterStateSupplier = clusterStateSupplier;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.rerouteService = rerouteService;
    }

    /**
     * Receives a copy of the latest {@link ClusterInfo} whenever the {@link ClusterInfoService} collects it. Processes the new
     * {@link org.elasticsearch.cluster.NodeUsageStatsForThreadPools} and initiates rebalancing, via reroute, if a node in the cluster
     * exceeds thread pool usage thresholds.
     */
    public void onNewInfo(ClusterInfo clusterInfo) {
        final ClusterState state = clusterStateSupplier.get();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("skipping monitor as the cluster state is not recovered yet");
            return;
        }

        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled() == WriteLoadConstraintSettings.WriteLoadDeciderStatus.DISABLED) {
            logger.trace("skipping monitor because the write load decider is disabled");
            return;
        }

        logger.trace("processing new cluster info");

        boolean reroute = false;
        String explanation = "";
        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();

        // TODO (ES-11992): implement

        if (reroute) {
            logger.debug("rerouting shards: [{}]", explanation);
            rerouteService.reroute("disk threshold monitor", Priority.NORMAL, ActionListener.wrap(ignored -> {
                final var reroutedClusterState = clusterStateSupplier.get();

                // TODO (ES-11992): implement

            }, e -> logger.debug("reroute failed", e)));
        } else {
            logger.trace("no reroute required");
        }
    }
}
