/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.function.Supplier;

public class DesiredBalanceShardsAllocator implements ShardsAllocator {

    private final BalancedShardsAllocator balancedShardsAllocator;
    private final Supplier<RerouteService> rerouteServiceSupplier;

    private DesiredBalance currentDesiredBalance;

    public DesiredBalanceShardsAllocator(
        Settings settings,
        ClusterSettings clusterSettings,
        Supplier<RerouteService> rerouteServiceSupplier
    ) {
        this.rerouteServiceSupplier = rerouteServiceSupplier;
        this.balancedShardsAllocator = new BalancedShardsAllocator(settings, clusterSettings);
    }

    @Override
    public void allocate(RoutingAllocation allocation) {
        assert MasterService.isMasterUpdateThread() || Thread.currentThread().getName().startsWith("TEST-")
            : Thread.currentThread().getName();

    }

    @Override
    public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
        return balancedShardsAllocator.decideShardAllocation(shard, allocation);
    }

    static class DesiredBalance {

    }
}
