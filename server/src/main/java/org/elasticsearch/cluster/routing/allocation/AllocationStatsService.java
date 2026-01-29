/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.transport.Transports;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Exposes cluster allocation metrics. Constructs {@link NodeAllocationStats} per node, on demand.
 */
public class AllocationStatsService {
    private final ClusterService clusterService;
    private final ClusterInfoService clusterInfoService;
    private final Supplier<DesiredBalance> desiredBalanceSupplier;
    private final NodeAllocationStatsAndWeightsCalculator nodeAllocationStatsAndWeightsCalculator;

    public AllocationStatsService(
        ClusterService clusterService,
        ClusterInfoService clusterInfoService,
        ShardsAllocator shardsAllocator,
        NodeAllocationStatsAndWeightsCalculator nodeAllocationStatsAndWeightsCalculator
    ) {
        this.clusterService = clusterService;
        this.clusterInfoService = clusterInfoService;
        this.nodeAllocationStatsAndWeightsCalculator = nodeAllocationStatsAndWeightsCalculator;
        this.desiredBalanceSupplier = shardsAllocator instanceof DesiredBalanceShardsAllocator allocator
            ? allocator::getDesiredBalance
            : () -> null;
    }

    /**
     * Returns a map of node IDs to node allocation stats.
     */
    public Map<String, NodeAllocationStats> stats() {
        return stats(() -> {});
    }

    /**
     * Returns a map of node IDs to node allocation stats, promising to execute the provided {@link Runnable} during the computation to
     * test for cancellation.
     */
    public Map<String, NodeAllocationStats> stats(Runnable ensureNotCancelled) {
        assert Transports.assertNotTransportThread("too expensive for a transport worker");

        var clusterState = clusterService.state();
        var nodesStatsAndWeights = nodeAllocationStatsAndWeightsCalculator.nodesAllocationStatsAndWeights(
            clusterState.metadata(),
            clusterState.getRoutingNodes(),
            clusterInfoService.getClusterInfo(),
            ensureNotCancelled,
            desiredBalanceSupplier.get()
        );
        return nodesStatsAndWeights.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new NodeAllocationStats(
                        e.getValue().shards(),
                        e.getValue().undesiredShards(),
                        e.getValue().forecastedIngestLoad(),
                        e.getValue().forecastedDiskUsage(),
                        e.getValue().currentDiskUsage()
                    )
                )
            );
    }
}
