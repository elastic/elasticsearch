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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A {@link ShardsAllocator} which asynchronously refreshes the desired balance held by the {@link DesiredBalanceComputer} and then takes
 * steps towards the desired balance using the {@link DesiredBalanceReconciler}.
 */
public class DesiredBalanceShardsAllocator implements ShardsAllocator {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceShardsAllocator.class);

    private final ShardsAllocator delegateAllocator;
    private final ClusterService clusterService;
    private final DesiredBalanceComputer desiredBalanceComputer;
    private final ContinuousComputation<DesiredBalanceInput> desiredBalanceComputation;
    private final PendingListenersQueue queue;
    private final AtomicLong indexGenerator = new AtomicLong(-1);
    private final ConcurrentLinkedQueue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves = new ConcurrentLinkedQueue<>();
    private volatile DesiredBalance currentDesiredBalance = DesiredBalance.INITIAL;// TODO convert to local var?

    @FunctionalInterface
    public interface DesiredBalanceReconcilerAction {
        ClusterState apply(ClusterState clusterState, Consumer<RoutingAllocation> routingAllocationAction);
    }

    public DesiredBalanceShardsAllocator(
        ShardsAllocator delegateAllocator,
        ThreadPool threadPool,
        ClusterService clusterService,
        DesiredBalanceReconcilerAction reconciler
    ) {
        this.delegateAllocator = delegateAllocator;
        this.clusterService = clusterService;
        this.desiredBalanceComputer = new DesiredBalanceComputer(delegateAllocator);
        this.desiredBalanceComputation = new ContinuousComputation<>(threadPool.generic()) {

            @Override
            protected void processInput(DesiredBalanceInput desiredBalanceInput) {

                logger.trace("Computing balance for [{}]", desiredBalanceInput.index());

                setCurrentDesiredBalance(
                    desiredBalanceComputer.compute(currentDesiredBalance, desiredBalanceInput, pendingDesiredBalanceMoves, this::isFresh)
                );
                if (isFresh(desiredBalanceInput)) {
                    logger.trace("Scheduling a reconciliation");
                    submitReconcileTask(currentDesiredBalance, reconciler);
                }
            }

            @Override
            public String toString() {
                return "DesiredBalanceShardsAllocator#updateDesiredBalanceAndReroute";
            }
        };
        this.queue = new PendingListenersQueue(threadPool);
    }

    public void submitReconcileTask(DesiredBalance desiredBalance, DesiredBalanceReconcilerAction reconciler) {
        // TODO batch
        clusterService.submitUnbatchedStateUpdateTask("reconcile", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                logger.trace("Executing reconcile for state [{}]:\n{}", currentState.version(), currentState);
                return reconciler.apply(
                    currentState,
                    routingAllocation -> new DesiredBalanceReconciler(desiredBalance, routingAllocation).run()
                );
            }

            @Override
            public void onFailure(Exception e) {
                assert MasterService.isPublishFailureException(e) : e;
                onNoLongerMaster();
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                queue.complete(desiredBalance.lastConvergedIndex());
            }
        });
    }

    @Override
    public void allocate(RoutingAllocation allocation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void allocate(RoutingAllocation allocation, ActionListener<Void> listener) {
        assert MasterService.assertMasterUpdateOrTestThread() : Thread.currentThread().getName();
        assert allocation.ignoreDisable() == false;
        // TODO add system context assertion
        // TODO must also capture any shards that the existing-shards allocators have allocated this pass, not just the ignored ones

        var index = indexGenerator.incrementAndGet();
        logger.trace("Executing allocate for [{}]", index);
        queue.add(index, listener);
        desiredBalanceComputation.onNewInput(
            new DesiredBalanceInput(index, allocation.immutableClone(), new ArrayList<>(allocation.routingNodes().unassigned().ignored()))
        );
    }

    @Override
    public RoutingExplanations execute(RoutingAllocation allocation, AllocationCommands commands, boolean explain, boolean retryFailed) {
        var explanations = ShardsAllocator.super.execute(allocation, commands, explain, retryFailed);
        var moves = getMoveCommands(commands);
        if (moves.isEmpty() == false) {
            pendingDesiredBalanceMoves.add(moves);
        }
        return explanations;
    }

    private static List<MoveAllocationCommand> getMoveCommands(AllocationCommands commands) {
        var moves = new ArrayList<MoveAllocationCommand>();
        for (AllocationCommand command : commands.commands()) {
            if (command instanceof MoveAllocationCommand move) {
                moves.add(move);
            }
        }
        return moves;
    }

    @Override
    public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
        return delegateAllocator.decideShardAllocation(shard, allocation);
    }

    private void onNoLongerMaster() {
        if (indexGenerator.getAndSet(-1) != -1) {
            currentDesiredBalance = DesiredBalance.INITIAL;
            queue.completeAllAsNotMaster();
            pendingDesiredBalanceMoves.clear();
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
}
