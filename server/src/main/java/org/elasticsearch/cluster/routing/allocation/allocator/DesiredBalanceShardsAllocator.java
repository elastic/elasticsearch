/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * A {@link ShardsAllocator} which asynchronously refreshes the desired balance held by the {@link DesiredBalanceService} and then takes
 * steps towards the desired balance using the {@link DesiredBalanceReconciler}.
 */
public class DesiredBalanceShardsAllocator implements ShardsAllocator, ClusterStateListener {

    private final Logger logger = LogManager.getLogger(DesiredBalanceShardsAllocator.class);

    public static final ActionListener<Void> REMOVE_ME = new ActionListener<>() {

        @Override
        public void onResponse(Void unused) {
            // TODO this is a noop listener stub that need so be replaced with a real implementation eventually
        }

        @Override
        public void onFailure(Exception e) {

        }

        @Override
        public String toString() {
            return "REMOVE_ME";
        }
    };

    private final ShardsAllocator delegateAllocator;
    private final ThreadPool threadPool;
    private final ContinuousComputation<DesiredBalanceInput> desiredBalanceComputation;
    private final DesiredBalanceService desiredBalanceService;

    private record DesiredBalancesListener(long index, ActionListener<Void> listener) {}

    private final AtomicLong indexGenerator = new AtomicLong(-1);
    private final Queue<DesiredBalancesListener> pendingListeners = new LinkedList<>();
    private volatile long lastConvergedIndex = -1;
    private volatile boolean allocating = false;

    public static DesiredBalanceShardsAllocator create(
        ShardsAllocator delegateAllocator,
        ThreadPool threadPool,
        ClusterService clusterService,
        Supplier<RerouteService> rerouteServiceSupplier
    ) {
        var allocator = new DesiredBalanceShardsAllocator(delegateAllocator, threadPool, rerouteServiceSupplier);
        clusterService.addListener(allocator);
        return allocator;
    }

    public DesiredBalanceShardsAllocator(
        ShardsAllocator delegateAllocator,
        ThreadPool threadPool,
        Supplier<RerouteService> rerouteServiceSupplier
    ) {
        this.delegateAllocator = delegateAllocator;
        this.threadPool = threadPool;
        this.desiredBalanceService = new DesiredBalanceService(delegateAllocator);
        this.desiredBalanceComputation = new ContinuousComputation<>(threadPool.generic()) {

            @Override
            protected void processInput(DesiredBalanceInput desiredBalanceInput) {

                logger.trace("Computing balance for [{}]", desiredBalanceInput.index());

                var shouldReroute = desiredBalanceService.updateDesiredBalanceAndReroute(desiredBalanceInput, this::isFresh);
                var isFresh = isFresh(desiredBalanceInput);
                var lastConvergedIndex = getCurrentDesiredBalance().lastConvergedIndex();

                logger.trace(
                    "Computed balance for [{}], isFresh={}, shouldReroute={}, lastConvergedIndex={}",
                    desiredBalanceInput.index(),
                    isFresh,
                    shouldReroute,
                    lastConvergedIndex
                );

                if (shouldReroute) {
                    rerouteServiceSupplier.get().reroute("desired balance changed", Priority.NORMAL, ActionListener.noop());
                } else if (allocating == false) {
                    logger.trace("Executing listeners up to [{}] as desired balance did not require reroute", lastConvergedIndex);
                    executeListeners(lastConvergedIndex);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void allocate(RoutingAllocation allocation, ActionListener<Void> listener) {
        assert MasterService.isMasterUpdateThread() || Thread.currentThread().getName().startsWith("TEST-")
            : Thread.currentThread().getName();
        // assert allocation.debugDecision() == false; set to true when called via the reroute API
        assert allocation.ignoreDisable() == false;
        // TODO add system context assertion

        // TODO must also capture any shards that the existing-shards allocators have allocated this pass, not just the ignored ones

        allocating = true;

        var index = indexGenerator.incrementAndGet();
        logger.trace("Executing allocate for [{}]", index);
        synchronized (pendingListeners) {
            pendingListeners.add(new DesiredBalancesListener(index, listener));
        }
        desiredBalanceComputation.onNewInput(
            new DesiredBalanceInput(index, allocation.immutableClone(), new ArrayList<>(allocation.routingNodes().unassigned().ignored()))
        );

        // TODO possibly add a bounded wait for the computation to complete?
        // Otherwise we will have to do a second cluster state update straight away.

        DesiredBalance currentDesiredBalance = getCurrentDesiredBalance();
        new DesiredBalanceReconciler(currentDesiredBalance, allocation).run();

        lastConvergedIndex = currentDesiredBalance.lastConvergedIndex();
        if (allocation.routingNodesChanged()) {
            logger.trace("Delaying execution listeners up to [{}] as routing nodes have changed", index);
            // Execute listeners after cluster state is applied
        } else {
            logger.trace("Executing listeners up to [{}] as routing nodes have not changed", lastConvergedIndex);
            executeListeners(lastConvergedIndex);
            allocating = false;
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        logger.trace("Executing listeners up to [{}] after cluster state was committed", lastConvergedIndex);
        executeListeners(lastConvergedIndex);
        allocating = false;
    }

    private void executeListeners(long convergedIndex) {
        var listeners = pollListeners(convergedIndex);
        if (listeners.isEmpty() == false) {
            threadPool.generic().execute(() -> ActionListener.onResponse(listeners, null));
        }
    }

    private Collection<ActionListener<Void>> pollListeners(long maxIndex) {
        var listeners = new ArrayList<ActionListener<Void>>();
        DesiredBalancesListener listener;
        synchronized (pendingListeners) {
            while ((listener = pendingListeners.peek()) != null && listener.index <= maxIndex) {
                listeners.add(pendingListeners.poll().listener);
            }
        }
        return listeners;
    }

    @Override
    public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
        return delegateAllocator.decideShardAllocation(shard, allocation);
    }

    DesiredBalance getCurrentDesiredBalance() {
        return desiredBalanceService.getCurrentDesiredBalance();
    }

    public boolean isIdle() {
        return desiredBalanceComputation.isActive() == false;
    }
}
