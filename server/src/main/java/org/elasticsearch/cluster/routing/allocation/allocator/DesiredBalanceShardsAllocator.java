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
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * A {@link ShardsAllocator} which asynchronously refreshes the desired balance held by the {@link DesiredBalanceComputer} and then takes
 * steps towards the desired balance using the {@link DesiredBalanceReconciler}.
 */
public class DesiredBalanceShardsAllocator implements ShardsAllocator, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceShardsAllocator.class);

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
    private final DesiredBalanceComputer desiredBalanceComputer;
    private final ContinuousComputation<DesiredBalanceInput> desiredBalanceComputation;
    private final PendingListenersQueue queue;
    private final AtomicLong indexGenerator = new AtomicLong(-1);
    private final ConcurrentLinkedQueue<MoveAllocationCommand> pendingDesiredBalanceMoves = new ConcurrentLinkedQueue<>();
    private volatile DesiredBalance currentDesiredBalance = DesiredBalance.INITIAL;
    private volatile DesiredBalance appliedDesiredBalance = DesiredBalance.INITIAL;

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
        this.desiredBalanceComputer = new DesiredBalanceComputer(delegateAllocator);
        this.desiredBalanceComputation = new ContinuousComputation<>(threadPool.generic()) {

            @Override
            protected void processInput(DesiredBalanceInput desiredBalanceInput) {

                logger.trace("Computing balance for [{}]", desiredBalanceInput.index());

                setCurrentDesiredBalance(
                    desiredBalanceComputer.compute(
                        currentDesiredBalance,
                        desiredBalanceInput,
                        pollAll(pendingDesiredBalanceMoves),
                        this::isFresh
                    )
                );
                var isFresh = isFresh(desiredBalanceInput);

                if (isFresh) {
                    if (DesiredBalance.hasChanges(currentDesiredBalance, appliedDesiredBalance)) {
                        logger.trace("Current desired balance is different from applied one, scheduling a reroute");
                        rerouteServiceSupplier.get().reroute("desired balance changed", Priority.NORMAL, ActionListener.noop());
                    } else {
                        var lastConvergedIndex = currentDesiredBalance.lastConvergedIndex();
                        logger.trace("Executing listeners up to [{}] as desired balance did not require reroute", lastConvergedIndex);
                        // TODO desired balance this still does not guarantee the correct behaviour in case there is
                        // extra unrelated allocation between one that triggered this computation and one produced by above reroute.
                        queue.complete(lastConvergedIndex);
                    }
                }
            }

            @Override
            public String toString() {
                return "DesiredBalanceShardsAllocator#updateDesiredBalanceAndReroute";
            }
        };
        this.queue = new PendingListenersQueue(threadPool);
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

        queue.pause();

        var index = indexGenerator.incrementAndGet();
        logger.trace("Executing allocate for [{}]", index);
        queue.add(index, listener);
        desiredBalanceComputation.onNewInput(
            new DesiredBalanceInput(index, allocation.immutableClone(), new ArrayList<>(allocation.routingNodes().unassigned().ignored()))
        );

        // TODO possibly add a bounded wait for the computation to complete?
        // Otherwise we will have to do a second cluster state update straight away.

        appliedDesiredBalance = currentDesiredBalance;
        logger.trace("Allocating using balance [{}]", appliedDesiredBalance);
        new DesiredBalanceReconciler(appliedDesiredBalance, allocation).run();

        queue.complete(appliedDesiredBalance.lastConvergedIndex());
        if (allocation.routingNodesChanged()) {
            logger.trace("Delaying execution listeners up to [{}] as routing nodes have changed", index);
            // Execute listeners after cluster state is applied
        } else {
            logger.trace("Executing listeners up to [{}] as routing nodes have not changed", queue.getCompletedIndex());
            queue.resume();
        }
    }

    @Override
    public RoutingExplanations execute(RoutingAllocation allocation, AllocationCommands commands, boolean explain, boolean retryFailed) {
        var explanations = ShardsAllocator.super.execute(allocation, commands, explain, retryFailed);
        for (AllocationCommand command : commands.commands()) {
            if (command instanceof MoveAllocationCommand move) {
                pendingDesiredBalanceMoves.add(move);
            }
        }
        return explanations;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().isLocalNodeElectedMaster()) {
            logger.trace("Executing listeners up to [{}] after cluster state was committed", queue.getCompletedIndex());
            queue.resume();
        } else {
            reset();
            queue.completeAllAsNotMaster();
            pollAll(pendingDesiredBalanceMoves);
        }
    }

    @Override
    public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
        return delegateAllocator.decideShardAllocation(shard, allocation);
    }

    private void reset() {
        if (indexGenerator.getAndSet(-1) != -1) {
            currentDesiredBalance = DesiredBalance.INITIAL;
            appliedDesiredBalance = DesiredBalance.INITIAL;
        }
    }

    private boolean setCurrentDesiredBalance(DesiredBalance newDesiredBalance) {
        boolean hasChanges = DesiredBalance.hasChanges(currentDesiredBalance, newDesiredBalance);
        if (logger.isTraceEnabled()) {
            if (hasChanges) {
                logger.trace("desired balance changed: {}\n{}", newDesiredBalance, diff(currentDesiredBalance, newDesiredBalance));
            } else {
                logger.trace("desired balance unchanged: {}", newDesiredBalance);
            }
        }
        currentDesiredBalance = newDesiredBalance;
        return hasChanges;
    }

    private static String diff(DesiredBalance old, DesiredBalance updated) {
        var intersection = Sets.intersection(old.assignments().keySet(), updated.assignments().keySet());
        var diff = Sets.difference(Sets.union(old.assignments().keySet(), updated.assignments().keySet()), intersection);

        var newLine = System.lineSeparator();
        var builder = new StringBuilder();
        for (ShardId shardId : intersection) {
            var oldAssignment = old.getAssignment(shardId);
            var updatedAssignment = updated.getAssignment(shardId);
            if (Objects.equals(oldAssignment, updatedAssignment) == false) {
                builder.append(newLine).append(shardId).append(": ").append(oldAssignment).append(" --> ").append(updatedAssignment);
            }
        }
        for (ShardId shardId : diff) {
            var oldAssignment = old.getAssignment(shardId);
            var updatedAssignment = updated.getAssignment(shardId);
            builder.append(newLine).append(shardId).append(": ").append(oldAssignment).append(" --> ").append(updatedAssignment);
        }
        return builder.append(newLine).toString();
    }

    private static <T> List<T> pollAll(Queue<T> queue) {
        var list = new ArrayList<T>();
        T item;
        while ((item = queue.poll()) != null) {
            list.add(item);
        }
        return list;
    }
}
