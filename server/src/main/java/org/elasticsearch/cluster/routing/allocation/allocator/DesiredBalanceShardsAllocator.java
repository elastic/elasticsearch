/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * A {@link ShardsAllocator} which asynchronously refreshes the desired balance held by the {@link DesiredBalanceService} and then takes
 * steps towards the desired balance using the {@link DesiredBalanceReconciler}.
 */
public class DesiredBalanceShardsAllocator implements ShardsAllocator {

    private final ShardsAllocator delegateAllocator;
    private final ContinuousComputation<DesiredBalanceInput> desiredBalanceComputation;
    private final DesiredBalanceService desiredBalanceService;

    public DesiredBalanceShardsAllocator(
        ShardsAllocator delegateAllocator,
        ThreadPool threadPool,
        Supplier<RerouteService> rerouteServiceSupplier
    ) {
        this.delegateAllocator = delegateAllocator;
        this.desiredBalanceService = new DesiredBalanceService(delegateAllocator);
        this.desiredBalanceComputation = new ContinuousComputation<>(threadPool.generic()) {
            @Override
            protected void processInput(DesiredBalanceInput desiredBalanceInput) {
                if (desiredBalanceService.updateDesiredBalanceAndReroute(desiredBalanceInput, () -> isFresh(desiredBalanceInput))) {
                    rerouteServiceSupplier.get().reroute("desired balance changed", Priority.NORMAL, ActionListener.wrap(() -> {}));
                }
            }

            @Override
            public String toString() {
                return "DesiredBalanceShardsAllocator#updateDesiredBalanceAndReroute";
            }
        };
    }

    @Override
    public void allocate(RoutingAllocation allocation) {
        assert MasterService.isMasterUpdateThread() || Thread.currentThread().getName().startsWith("TEST-")
            : Thread.currentThread().getName();
        // assert allocation.debugDecision() == false; set to true when called via the reroute API
        assert allocation.ignoreDisable() == false;

        // TODO must also capture any shards that the existing-shards allocators have allocated this pass, not just the ignored ones

        desiredBalanceComputation.onNewInput(
            new DesiredBalanceInput(allocation.immutableClone(), new ArrayList<>(allocation.routingNodes().unassigned().ignored()))
        );

        // TODO possibly add a bounded wait for the computation to complete?
        // Otherwise we will have to do a second cluster state update straight away.

        new DesiredBalanceReconciler(getCurrentDesiredBalance(), allocation).run();

    }

    @Override
    public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
        return delegateAllocator.decideShardAllocation(shard, allocation);
    }

    DesiredBalance getCurrentDesiredBalance() {
        return desiredBalanceService.getCurrentDesiredBalance();
    }

}
