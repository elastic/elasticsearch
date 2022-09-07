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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A {@link ShardsAllocator} which asynchronously refreshes the desired balance held by the {@link DesiredBalanceComputer} and then takes
 * steps towards the desired balance using the {@link DesiredBalanceReconciler}.
 */
public class DesiredBalanceShardsAllocator implements ShardsAllocator, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceShardsAllocator.class);

    private final ShardsAllocator delegateAllocator;
    private final DesiredBalanceComputer desiredBalanceComputer;
    private final ContinuousComputation<DesiredBalanceInput> desiredBalanceComputation;
    private final PendingListenersQueue queue;
    private final AtomicLong indexGenerator = new AtomicLong(-1);
    private final ConcurrentLinkedQueue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves = new ConcurrentLinkedQueue<>();
    private volatile DesiredBalance currentDesiredBalance = DesiredBalance.INITIAL;
    private volatile DesiredBalance appliedDesiredBalance = DesiredBalance.INITIAL;

    public static DesiredBalanceShardsAllocator create(
        ShardsAllocator delegateAllocator,
        ThreadPool threadPool,
        ClusterService clusterService,
        Supplier<RerouteService> rerouteServiceSupplier,
        BiFunction<ClusterState, Long, RoutingAllocation> routingAllocationCreator
    ) {
        var allocator = new DesiredBalanceShardsAllocator(
            delegateAllocator,
            threadPool,
            rerouteServiceSupplier,
            clusterService,
            routingAllocationCreator
        );
        clusterService.addListener(allocator);
        return allocator;
    }

    public DesiredBalanceShardsAllocator(
        ShardsAllocator delegateAllocator,
        ThreadPool threadPool,
        Supplier<RerouteService> rerouteServiceSupplier,
        ClusterService clusterService,
        BiFunction<ClusterState, Long, RoutingAllocation> routingAllocationCreator
    ) {
        this.delegateAllocator = delegateAllocator;
        this.desiredBalanceComputer = new DesiredBalanceComputer(delegateAllocator);
        this.desiredBalanceComputation = new ContinuousComputation<>(threadPool.generic()) {

            @Override
            protected void processInput(DesiredBalanceInput desiredBalanceInput) {

                logger.trace("Computing balance for [{}]", desiredBalanceInput.index());

                setCurrentDesiredBalance(
                    desiredBalanceComputer.compute(currentDesiredBalance, desiredBalanceInput, pendingDesiredBalanceMoves, this::isFresh)
                );
                var isFresh = isFresh(desiredBalanceInput);

                if (isFresh) {
                    // if (DesiredBalance.hasChanges(currentDesiredBalance, appliedDesiredBalance)) {
                    logger.trace("scheduling a reconciliation");
                    var desiredBalanceForReconcilation = currentDesiredBalance;
                    // TODO implement batching
                    clusterService.submitUnbatchedStateUpdateTask("reconcile", new ClusterStateUpdateTask() {
                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            logger.info("--> executing reconcile for state [{}]:\n{}", currentState.version(), currentState);
                            var success = false;
                            try {
                                final var routingAllocation = routingAllocationCreator.apply(currentState, System.nanoTime());
                                new DesiredBalanceReconciler(desiredBalanceForReconcilation, routingAllocation).run();
                                final var unassignedIterator = routingAllocation.routingNodes().unassigned().iterator();
                                while (unassignedIterator.hasNext()) {
                                    final var shardRouting = unassignedIterator.next();
                                    logger.info("--> ignoring unassigned [{}]", shardRouting);
                                    unassignedIterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                                        routingAllocation.changes());
                                }
                                final var newState = routingAllocation.routingNodesChanged() ?
                                    AllocationService.buildResultAndLogHealthChange(currentState, routingAllocation, "reconcile")
                                    : currentState;
                                success = true;
                                return newState;
                            } finally {
                                    logger.info("--> executing reconcile for state [{}] {}", currentState.version(),
                                        success ? "succeeded" : "failed");
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            queue.completeAllAsNotMaster();
                        }

                        @Override
                        public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                            queue.complete(desiredBalanceForReconcilation.lastConvergedIndex());
                            // reconciliation has started, complete listeners

                        }
                    });
                    // rerouteServiceSupplier.get().reroute("desired balance changed", Priority.NORMAL, ActionListener.noop());
                    // } else {
                    // var lastConvergedIndex = currentDesiredBalance.lastConvergedIndex();
                    // logger.trace("Executing listeners up to [{}] as desired balance did not require reroute", lastConvergedIndex);
                    // // TODO desired balance this still does not guarantee the correct behaviour in case there is
                    // // extra unrelated allocation between one that triggered this computation and one produced by above reroute.
                    // queue.complete(lastConvergedIndex);
                    // }
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
        assert MasterService.assertMasterUpdateOrTestThread() : Thread.currentThread().getName();
        assert allocation.ignoreDisable() == false;
        // TODO add system context assertion
        // TODO must also capture any shards that the existing-shards allocators have allocated this pass, not just the ignored ones

        // queue.pause();

        var index = indexGenerator.incrementAndGet();
        logger.trace("Executing allocate for [{}]", index);
        queue.add(index, listener);
        desiredBalanceComputation.onNewInput(
            new DesiredBalanceInput(index, allocation.immutableClone(), new ArrayList<>(allocation.routingNodes().unassigned().ignored()))
        );

        maybeAwaitBalance();

        appliedDesiredBalance = currentDesiredBalance;
        //// logger.trace("Allocating using balance [{}]", appliedDesiredBalance);
        //// new DesiredBalanceReconciler(appliedDesiredBalance, allocation).run();
        ////
        //// queue.complete(appliedDesiredBalance.lastConvergedIndex());
        // if (allocation.routingNodesChanged()) {
        // logger.trace("Delaying execution listeners up to [{}] as routing nodes have changed", index);
        // // Execute listeners after cluster state is applied
        // } else {
        // logger.trace("Executing listeners up to [{}] as routing nodes have not changed", queue.getCompletedIndex());
        // queue.resume();
        // }
    }

    protected void maybeAwaitBalance() {
        // TODO possibly add a bounded wait for the computation to complete?
        // Otherwise we will have to do a second cluster state update straight away.
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
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().isLocalNodeElectedMaster()) {
            logger.trace("Executing listeners up to [{}] after cluster state was committed", queue.getCompletedIndex());
            queue.resume();
        } else {
            reset();
            queue.completeAllAsNotMaster();
            pendingDesiredBalanceMoves.clear();
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
}
