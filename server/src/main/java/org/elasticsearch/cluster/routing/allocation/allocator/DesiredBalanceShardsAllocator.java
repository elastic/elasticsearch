/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.AllocationService.RerouteStrategy;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStatsAndWeightsCalculator;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link ShardsAllocator} which asynchronously refreshes the desired balance held by the {@link DesiredBalanceComputer} and then takes
 * steps towards the desired balance using the {@link DesiredBalanceReconciler}.
 */
public class DesiredBalanceShardsAllocator implements ShardsAllocator {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceShardsAllocator.class);

    private final ShardsAllocator delegateAllocator;
    private final ThreadPool threadPool;
    /**
     * This is a callback to run {@link AllocationService#executeWithRoutingAllocation(ClusterState, String, RerouteStrategy)}, which
     * produces a new ClusterState with the changes made by {@link DesiredBalanceReconciler#reconcile}. The {@link RerouteStrategy} provided
     * to the callback calls into {@link #desiredBalanceReconciler} for the changes. The {@link #masterServiceTaskQueue} will publish the
     * new cluster state after the cluster state is constructed by the {@link ReconcileDesiredBalanceExecutor}.
     */
    private final DesiredBalanceReconcilerAction reconciler;
    private final DesiredBalanceComputer desiredBalanceComputer;
    /**
     * Reconciliation ({@link DesiredBalanceReconciler#reconcile(DesiredBalance, RoutingAllocation)}) takes the {@link DesiredBalance}
     * output of {@link DesiredBalanceComputer#compute} and identifies how shards need to be added, moved or removed to go from the current
     * cluster shard allocation to the new desired allocation.
     */
    private final DesiredBalanceReconciler desiredBalanceReconciler;
    private final ContinuousComputation<DesiredBalanceInput> desiredBalanceComputation;
    /**
     * Saves and runs listeners after DesiredBalance computations complete.
     */
    private final PendingListenersQueue pendingListenersQueue;
    /**
     * Each reroute request gets assigned a monotonically increasing sequence number. Many reroute requests may arrive before the balancer
     * asynchronously runs a computation. The balancer will use the latest request and save this sequence number to track back to the
     * request.
     */
    private final AtomicLong indexGenerator = new AtomicLong(-1);
    private final ConcurrentLinkedQueue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves = new ConcurrentLinkedQueue<>();
    private final MasterServiceTaskQueue<ReconcileDesiredBalanceTask> masterServiceTaskQueue;
    private final AtomicReference<DesiredBalance> currentDesiredBalanceRef = new AtomicReference<>(DesiredBalance.NOT_MASTER);
    private volatile boolean resetCurrentDesiredBalance = false;
    private final Set<String> processedNodeShutdowns = new HashSet<>();
    private final NodeAllocationStatsAndWeightsCalculator nodeAllocationStatsAndWeightsCalculator;
    private final DesiredBalanceMetrics desiredBalanceMetrics;
    /**
     * Manages balancer round results in order to report on the balancer activity in a configurable manner.
     */
    private final AllocationBalancingRoundSummaryService balancerRoundSummaryService;

    // stats
    protected final CounterMetric computationsSubmitted = new CounterMetric();
    protected final CounterMetric computationsExecuted = new CounterMetric();
    protected final CounterMetric computationsConverged = new CounterMetric();
    protected final MeanMetric computedShardMovements = new MeanMetric();
    protected final CounterMetric cumulativeComputationTime = new CounterMetric();
    protected final CounterMetric cumulativeReconciliationTime = new CounterMetric();

    @FunctionalInterface
    public interface DesiredBalanceReconcilerAction {
        ClusterState apply(ClusterState clusterState, RerouteStrategy rerouteStrategy);
    }

    public DesiredBalanceShardsAllocator(
        ClusterSettings clusterSettings,
        ShardsAllocator delegateAllocator,
        ThreadPool threadPool,
        ClusterService clusterService,
        DesiredBalanceReconcilerAction reconciler,
        TelemetryProvider telemetryProvider,
        NodeAllocationStatsAndWeightsCalculator nodeAllocationStatsAndWeightsCalculator
    ) {
        this(
            delegateAllocator,
            threadPool,
            clusterService,
            new DesiredBalanceComputer(clusterSettings, threadPool, delegateAllocator),
            reconciler,
            telemetryProvider,
            nodeAllocationStatsAndWeightsCalculator
        );
    }

    public DesiredBalanceShardsAllocator(
        ShardsAllocator delegateAllocator,
        ThreadPool threadPool,
        ClusterService clusterService,
        DesiredBalanceComputer desiredBalanceComputer,
        DesiredBalanceReconcilerAction reconciler,
        TelemetryProvider telemetryProvider,
        NodeAllocationStatsAndWeightsCalculator nodeAllocationStatsAndWeightsCalculator
    ) {
        this.desiredBalanceMetrics = new DesiredBalanceMetrics(telemetryProvider.getMeterRegistry());
        this.nodeAllocationStatsAndWeightsCalculator = nodeAllocationStatsAndWeightsCalculator;
        this.balancerRoundSummaryService = new AllocationBalancingRoundSummaryService(threadPool, clusterService.getClusterSettings());
        this.delegateAllocator = delegateAllocator;
        this.threadPool = threadPool;
        this.reconciler = reconciler;
        this.desiredBalanceComputer = desiredBalanceComputer;
        this.desiredBalanceReconciler = new DesiredBalanceReconciler(clusterService.getClusterSettings(), threadPool);
        this.desiredBalanceComputation = new ContinuousComputation<>(threadPool.generic()) {

            @Override
            protected void processInput(DesiredBalanceInput desiredBalanceInput) {
                processNodeShutdowns(desiredBalanceInput.routingAllocation().getClusterState());

                long index = desiredBalanceInput.index();
                logger.debug("Starting desired balance computation for [{}]", index);

                final DesiredBalance initialDesiredBalance = getInitialDesiredBalance();
                if (initialDesiredBalance == DesiredBalance.NOT_MASTER) {
                    logger.debug("Abort desired balance computation because node is no longer master");
                    return;
                }

                recordTime(
                    cumulativeComputationTime,
                    // We set currentDesiredBalance back to INITIAL when the node stands down as master in onNoLongerMaster.
                    // However, it is possible that we revert the effect here by setting it again since the computation is async
                    // and does not check whether the node is master. This should have little to no practical impact. But it may
                    // lead to unexpected behaviours for tests. See also https://github.com/elastic/elasticsearch/pull/116904
                    () -> setCurrentDesiredBalance(
                        desiredBalanceComputer.compute(
                            initialDesiredBalance,
                            desiredBalanceInput,
                            pendingDesiredBalanceMoves,
                            this::isFresh
                        )
                    )
                );
                computationsExecuted.inc();

                final DesiredBalance currentDesiredBalance = currentDesiredBalanceRef.get();
                if (currentDesiredBalance == DesiredBalance.NOT_MASTER || currentDesiredBalance == DesiredBalance.BECOME_MASTER_INITIAL) {
                    logger.debug(
                        () -> Strings.format(
                            "Desired balance computation for [%s] is discarded since master has concurrently changed. "
                                + "Current desiredBalance=[%s]",
                            index,
                            currentDesiredBalance
                        )
                    );
                } else if (currentDesiredBalance.finishReason() == DesiredBalance.ComputationFinishReason.STOP_EARLY) {
                    logger.debug(
                        "Desired balance computation for [{}] terminated early with partial result, scheduling reconciliation",
                        index
                    );
                    submitReconcileTask(currentDesiredBalance);
                    var newInput = DesiredBalanceInput.create(indexGenerator.incrementAndGet(), desiredBalanceInput.routingAllocation());
                    desiredBalanceComputation.compareAndEnqueue(desiredBalanceInput, newInput);
                } else if (isFresh(desiredBalanceInput)) {
                    logger.debug("Desired balance computation for [{}] is completed, scheduling reconciliation", index);
                    computationsConverged.inc();
                    submitReconcileTask(currentDesiredBalance);
                } else {
                    logger.debug("Desired balance computation for [{}] is discarded as newer one is submitted", index);
                }
            }

            private DesiredBalance getInitialDesiredBalance() {
                final DesiredBalance currentDesiredBalance = currentDesiredBalanceRef.get();
                if (resetCurrentDesiredBalance) {
                    logger.info("Resetting current desired balance");
                    resetCurrentDesiredBalance = false;
                    return currentDesiredBalance == DesiredBalance.NOT_MASTER
                        ? DesiredBalance.NOT_MASTER
                        : new DesiredBalance(currentDesiredBalance.lastConvergedIndex(), Map.of());
                } else {
                    return currentDesiredBalance;
                }
            }

            @Override
            public String toString() {
                return "DesiredBalanceShardsAllocator#allocate";
            }
        };
        this.pendingListenersQueue = new PendingListenersQueue();
        this.masterServiceTaskQueue = clusterService.createTaskQueue(
            "reconcile-desired-balance",
            Priority.URGENT,
            new ReconcileDesiredBalanceExecutor()
        );
        clusterService.addListener(event -> {
            if (event.localNodeMaster() == false) {
                onNoLongerMaster();
            }
            // Only update on change, to minimise volatile writes
            if (event.localNodeMaster() != event.previousState().nodes().isLocalNodeElectedMaster()) {
                desiredBalanceMetrics.setNodeIsMaster(event.localNodeMaster());
            }
        });
    }

    @Override
    public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
        return delegateAllocator.decideShardAllocation(shard, allocation);
    }

    @Override
    public void allocate(RoutingAllocation allocation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void allocate(RoutingAllocation allocation, ActionListener<Void> listener) {
        assert MasterService.assertMasterUpdateOrTestThread() : Thread.currentThread().getName();
        assert allocation.ignoreDisable() == false;

        computationsSubmitted.inc();

        var index = indexGenerator.incrementAndGet();
        logger.debug("Executing allocate for [{}]", index);
        pendingListenersQueue.add(index, listener);
        // This can only run on master, so unset not-master if exists
        if (currentDesiredBalanceRef.compareAndSet(DesiredBalance.NOT_MASTER, DesiredBalance.BECOME_MASTER_INITIAL)) {
            logger.debug("initialized desired balance for becoming master");
        }
        desiredBalanceComputation.onNewInput(DesiredBalanceInput.create(index, allocation));

        if (allocation.globalRoutingTable().hasIndices() == false) {
            logger.debug("No eager reconciliation needed for empty routing table");
            return;
        }
        // Starts reconciliation towards desired balance that might have not been updated with a recent calculation yet.
        // This is fine as balance should have incremental rather than radical changes.
        // This should speed up achieving the desired balance in cases current state is still different from it (due to THROTTLING).
        reconcile(currentDesiredBalanceRef.get(), allocation);
    }

    private void processNodeShutdowns(ClusterState clusterState) {
        final var nodes = clusterState.nodes();
        final var nodeShutdowns = clusterState.metadata().nodeShutdowns();
        // If we remove a shutdown marker from a node, but it is still in the cluster, we'd need a reset.
        boolean reset = processedNodeShutdowns.stream()
            .anyMatch(nodeId -> nodeShutdowns.contains(nodeId) == false && nodes.get(nodeId) != null);
        // Clean up processed shutdowns that are removed from the cluster metadata
        processedNodeShutdowns.removeIf(nodeId -> nodeShutdowns.contains(nodeId) == false);

        for (var shutdown : nodeShutdowns.getAll().entrySet()) {
            if (shutdown.getValue().getType() != SingleNodeShutdownMetadata.Type.RESTART) {
                reset |= processedNodeShutdowns.add(shutdown.getKey());
            }
        }
        if (reset) {
            resetDesiredBalance();
        }
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

    private void setCurrentDesiredBalance(DesiredBalance newDesiredBalance) {
        while (true) {
            final var oldDesiredBalance = currentDesiredBalanceRef.get();
            if (oldDesiredBalance == DesiredBalance.NOT_MASTER) {
                logger.debug("discard desired balance for [{}] since node is no longer master", newDesiredBalance.lastConvergedIndex());
                return;
            }

            if (currentDesiredBalanceRef.compareAndSet(oldDesiredBalance, newDesiredBalance)) {
                balancerRoundSummaryService.addBalancerRoundSummary(oldDesiredBalance, newDesiredBalance);
                if (logger.isTraceEnabled()) {
                    var diff = DesiredBalance.hasChanges(oldDesiredBalance, newDesiredBalance)
                        ? "Diff: " + DesiredBalance.humanReadableDiff(oldDesiredBalance, newDesiredBalance)
                        : "No changes";
                    logger.trace("Desired balance updated: {}. {}", newDesiredBalance, diff);
                } else {
                    logger.debug("Desired balance updated for [{}]", newDesiredBalance.lastConvergedIndex());
                }
                computedShardMovements.inc(DesiredBalance.shardMovements(oldDesiredBalance, newDesiredBalance));
                break;
            }
        }
    }

    /**
     * Submits the desired balance to be reconciled (applies the desired changes to the routing table) and creates and publishes a new
     * cluster state. The data nodes will receive and apply the new cluster state to start/move/remove shards.
     */
    protected void submitReconcileTask(DesiredBalance desiredBalance) {
        masterServiceTaskQueue.submitTask("reconcile-desired-balance", new ReconcileDesiredBalanceTask(desiredBalance), null);
    }

    protected void reconcile(DesiredBalance desiredBalance, RoutingAllocation allocation) {
        if (logger.isTraceEnabled()) {
            logger.trace("Reconciling desired balance: {}", desiredBalance);
        } else {
            logger.debug("Reconciling desired balance for [{}]", desiredBalance.lastConvergedIndex());
        }
        recordTime(cumulativeReconciliationTime, () -> {
            DesiredBalanceMetrics.AllocationStats allocationStats = desiredBalanceReconciler.reconcile(desiredBalance, allocation);
            updateDesireBalanceMetrics(desiredBalance, allocation, allocationStats);
        });

        if (logger.isTraceEnabled()) {
            logger.trace("Reconciled desired balance: {}", desiredBalance);
        } else {
            logger.debug("Reconciled desired balance for [{}]", desiredBalance.lastConvergedIndex());
        }
    }

    private RerouteStrategy createReconcileAllocationAction(DesiredBalance desiredBalance) {
        return new RerouteStrategy() {
            @Override
            public void removeDelayMarkers(RoutingAllocation allocation) {
                // it is possible that desired balance is computed before some delayed allocations are expired but reconciled after.
                // If delayed markers are removed during reconciliation then
                // * shards are not assigned anyway as balance is not computed for them
                // * followup reroute is not scheduled to allocate them
                // for this reason we should keep delay markers during reconciliation
            }

            @Override
            public void execute(RoutingAllocation allocation) {
                reconcile(desiredBalance, allocation);
            }
        };
    }

    public DesiredBalance getDesiredBalance() {
        return currentDesiredBalanceRef.get();
    }

    public void resetDesiredBalance() {
        resetCurrentDesiredBalance = true;
    }

    private void updateDesireBalanceMetrics(
        DesiredBalance desiredBalance,
        RoutingAllocation routingAllocation,
        DesiredBalanceMetrics.AllocationStats allocationStats
    ) {
        var nodesStatsAndWeights = nodeAllocationStatsAndWeightsCalculator.nodesAllocationStatsAndWeights(
            routingAllocation.metadata(),
            routingAllocation.routingNodes(),
            routingAllocation.clusterInfo(),
            desiredBalance
        );
        Map<DiscoveryNode, NodeAllocationStatsAndWeightsCalculator.NodeAllocationStatsAndWeight> filteredNodeAllocationStatsAndWeights =
            new HashMap<>(nodesStatsAndWeights.size());
        for (var nodeStatsAndWeight : nodesStatsAndWeights.entrySet()) {
            var node = routingAllocation.nodes().get(nodeStatsAndWeight.getKey());
            if (node != null) {
                filteredNodeAllocationStatsAndWeights.put(node, nodeStatsAndWeight.getValue());
            }
        }
        desiredBalanceMetrics.updateMetrics(allocationStats, desiredBalance.weightsPerNode(), filteredNodeAllocationStatsAndWeights);
    }

    public DesiredBalanceStats getStats() {
        return new DesiredBalanceStats(
            Math.max(currentDesiredBalanceRef.get().lastConvergedIndex(), 0L),
            desiredBalanceComputation.isActive(),
            computationsSubmitted.count(),
            computationsExecuted.count(),
            computationsConverged.count(),
            desiredBalanceComputer.iterations.sum(),
            computedShardMovements.sum(),
            cumulativeComputationTime.count(),
            cumulativeReconciliationTime.count(),
            desiredBalanceMetrics.unassignedShards(),
            desiredBalanceMetrics.totalAllocations(),
            desiredBalanceMetrics.undesiredAllocations()
        );
    }

    private void onNoLongerMaster() {
        if (indexGenerator.getAndSet(-1) != -1) {
            currentDesiredBalanceRef.set(DesiredBalance.NOT_MASTER);
            pendingListenersQueue.completeAllAsNotMaster();
            pendingDesiredBalanceMoves.clear();
            desiredBalanceReconciler.clear();
            desiredBalanceMetrics.zeroAllMetrics();
        }
    }

    private static final class ReconcileDesiredBalanceTask implements ClusterStateTaskListener {
        private final DesiredBalance desiredBalance;

        private ReconcileDesiredBalanceTask(DesiredBalance desiredBalance) {
            this.desiredBalance = desiredBalance;
        }

        @Override
        public void onFailure(Exception e) {
            assert MasterService.isPublishFailureException(e) : e;
        }

        @Override
        public String toString() {
            return "ReconcileDesiredBalanceTask[lastConvergedIndex=" + desiredBalance.lastConvergedIndex() + "]";
        }
    }

    private final class ReconcileDesiredBalanceExecutor implements ClusterStateTaskExecutor<ReconcileDesiredBalanceTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<ReconcileDesiredBalanceTask> batchExecutionContext) {
            var latest = findLatest(batchExecutionContext.taskContexts());
            var newState = applyBalance(batchExecutionContext, latest);
            discardSupersededTasks(batchExecutionContext.taskContexts(), latest);
            return newState;
        }

        private static TaskContext<ReconcileDesiredBalanceTask> findLatest(
            List<? extends TaskContext<ReconcileDesiredBalanceTask>> taskContexts
        ) {
            return taskContexts.stream().max(Comparator.comparing(context -> context.getTask().desiredBalance.lastConvergedIndex())).get();
        }

        private ClusterState applyBalance(
            BatchExecutionContext<ReconcileDesiredBalanceTask> batchExecutionContext,
            TaskContext<ReconcileDesiredBalanceTask> latest
        ) {
            try (var ignored = batchExecutionContext.dropHeadersContext()) {
                var newState = reconciler.apply(
                    batchExecutionContext.initialState(),
                    createReconcileAllocationAction(latest.getTask().desiredBalance)
                );
                latest.success(() -> pendingListenersQueue.complete(latest.getTask().desiredBalance.lastConvergedIndex()));
                return newState;
            }
        }

        private static void discardSupersededTasks(
            List<? extends TaskContext<ReconcileDesiredBalanceTask>> taskContexts,
            TaskContext<ReconcileDesiredBalanceTask> latest
        ) {
            for (TaskContext<ReconcileDesiredBalanceTask> taskContext : taskContexts) {
                if (taskContext != latest) {
                    taskContext.success(() -> {});
                }
            }
        }
    }

    // only for tests - in production, this happens after reconciliation
    protected final void completeToLastConvergedIndex() {
        pendingListenersQueue.complete(currentDesiredBalanceRef.get().lastConvergedIndex());
    }

    private void recordTime(CounterMetric metric, Runnable action) {
        final long started = threadPool.relativeTimeInMillis();
        try {
            action.run();
        } finally {
            final long finished = threadPool.relativeTimeInMillis();
            metric.inc(finished - started);
        }
    }

    // Visible for testing
    Set<String> getProcessedNodeShutdowns() {
        return Set.copyOf(processedNodeShutdowns);
    }
}
