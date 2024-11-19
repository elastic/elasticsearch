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

import java.util.Map;
import java.util.function.Supplier;

public class AllocationStatsService {
    private final ClusterService clusterService;
    private final ClusterInfoService clusterInfoService;
    private final Supplier<DesiredBalance> desiredBalanceSupplier;
    private final NodeAllocationStatsProvider nodeAllocationStatsProvider;

    public AllocationStatsService(
        ClusterService clusterService,
        ClusterInfoService clusterInfoService,
        ShardsAllocator shardsAllocator,
        NodeAllocationStatsProvider nodeAllocationStatsProvider
    ) {
        this.clusterService = clusterService;
        this.clusterInfoService = clusterInfoService;
        this.nodeAllocationStatsProvider = nodeAllocationStatsProvider;
        this.desiredBalanceSupplier = shardsAllocator instanceof DesiredBalanceShardsAllocator allocator
            ? allocator::getDesiredBalance
            : () -> null;
    }

    public Map<String, NodeAllocationStats> stats() {
        return nodeAllocationStatsProvider.stats(clusterService.state(), clusterInfoService.getClusterInfo(), desiredBalanceSupplier.get());
    }
}
