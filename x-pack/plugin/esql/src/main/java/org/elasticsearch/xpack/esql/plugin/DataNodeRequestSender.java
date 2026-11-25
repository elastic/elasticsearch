/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.HashMap.newHashMap;
import static org.elasticsearch.core.TimeValue.timeValueNanos;

/**
 * Handles computes within a single cluster by dispatching {@link DataNodeRequest} to data nodes
 * and executing these computes on the data nodes.
 */
abstract class DataNodeRequestSender {

    private static final Logger LOGGER = LogManager.getLogger(DataNodeRequestSender.class);
    private static final org.apache.logging.log4j.Logger OBSERVER_LOGGER = org.apache.logging.log4j.LogManager.getLogger(
        DataNodeRequestSender.class
    );

    /**
     * Query order according to the
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/node-roles-overview.html">node roles</a>.
     */
    private static final List<String> NODE_QUERY_ORDER = List.of(
        DiscoveryNodeRole.SEARCH_ROLE.roleName(),
        DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE.roleName(),
        DiscoveryNodeRole.DATA_HOT_NODE_ROLE.roleName(),
        DiscoveryNodeRole.DATA_WARM_NODE_ROLE.roleName(),
        DiscoveryNodeRole.DATA_COLD_NODE_ROLE.roleName(),
        DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName()
    );

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final TransportService transportService;
    private final Executor searchExecutor;
    private final CancellableTask rootTask;
    private final ThreadPool threadPool;
    private final TimeValue routingWaitTimeout;
    private final ShardResolutionMetrics metrics;
    /**
     * The project of the query, read once from the thread that builds the sender, which carries the query's context. Every routing
     * lookup made later takes this id. Some of them run where that context is not installed: the predicate of a routing wait runs
     * on the cluster applier thread, where {@link ProjectResolver} would name another project or throw.
     */
    private final ProjectId projectId;

    private final String clusterAlias;
    private final OriginalIndices originalIndices;
    private final QueryBuilder requestFilter;
    private final SliceSelection slices;

    private final boolean allowPartialResults;
    private final Semaphore concurrentRequests;
    private final ReentrantLock sendingLock = new ReentrantLock();
    private final Queue<ShardId> pendingShardIds = ConcurrentCollections.newQueue();
    private final Map<DiscoveryNode, Semaphore> nodePermits = new HashMap<>();
    private final Map<ShardId, ShardFailure> shardFailures = ConcurrentCollections.newConcurrentMap();
    private final Set<DiscoveryNode> refusedNodes = ConcurrentCollections.newConcurrentSet();
    /**
     * The copies of a shard that failed as unavailable, by {@link AllocationId#getId}. The value says whether the routing showed
     * the copy active when the request was sent: a copy tried while initializing is untried again once active.
     * <p>
     * Copies are told apart by allocation id and not by node, so that a shard that comes back on a node that failed it before, under
     * a new allocation, is tried there again, while the same copy is not asked twice under unchanged routing.
     * <p>
     * Concurrent: written by response handlers outside {@link #sendingLock}, read by the sending loop and, through the predicate of
     * a routing wait, by the cluster applier thread.
     */
    private final Map<ShardId, Map<String, Boolean>> triedCopies = ConcurrentCollections.newConcurrentMap();
    private final AtomicInteger skippedShards = new AtomicInteger();
    private final AtomicBoolean changed = new AtomicBoolean();
    // Written under sendingLock, but also read without the lock by RoutingWait#start(), so that a wait polled from
    // waitsToStart but not yet added to activeWaits still ends with QUERY_FAILED instead of running until timeout.
    private volatile boolean reportedFailure = false;
    private final AtomicInteger remainingUnavailableShardResolutionAttempts;
    private final AtomicBoolean attemptsExhaustedCounted = new AtomicBoolean();

    private final Set<ShardId> waitingShards = new HashSet<>(); // guarded by sendingLock
    /**
     * When the stall of a shard ends, in relative millis, or {@link #STALL_EXPIRED}. Guarded by {@link #sendingLock}.
     */
    private final Map<ShardId, Long> stallDeadlines = new HashMap<>();
    /**
     * Marks a stall whose wait timed out. The pass that follows gives the shard its last try and does not compare clocks again:
     * the relative time is a cached clock, and could still read as before the deadline.
     */
    private static final long STALL_EXPIRED = Long.MIN_VALUE;
    private boolean clusterServiceClosed; // guarded by sendingLock
    private final Queue<EndedWait> endedWaits = ConcurrentCollections.newQueue();
    private final Queue<RoutingWait> waitsToStart = ConcurrentCollections.newQueue();
    private final Set<RoutingWait> activeWaits = ConcurrentCollections.newConcurrentSet();
    private final AtomicBoolean hooksRegistered = new AtomicBoolean();

    DataNodeRequestSender(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        TransportService transportService,
        Executor searchExecutor,
        CancellableTask rootTask,
        OriginalIndices originalIndices,
        QueryBuilder requestFilter,
        SliceSelection slices,
        String clusterAlias,
        boolean allowPartialResults,
        int concurrentRequests,
        int unavailableShardResolutionAttempts,
        TimeValue routingWaitTimeout,
        ShardResolutionMetrics metrics
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.projectId = projectResolver.getProjectId();
        this.transportService = transportService;
        this.searchExecutor = searchExecutor;
        this.rootTask = rootTask;
        this.threadPool = transportService.getThreadPool();
        this.routingWaitTimeout = routingWaitTimeout;
        this.metrics = metrics;
        this.originalIndices = originalIndices;
        this.requestFilter = requestFilter;
        this.slices = slices;
        this.clusterAlias = clusterAlias;
        this.allowPartialResults = allowPartialResults;
        this.concurrentRequests = concurrentRequests > 0 ? new Semaphore(concurrentRequests) : null;
        this.remainingUnavailableShardResolutionAttempts = new AtomicInteger(
            unavailableShardResolutionAttempts >= 0 ? unavailableShardResolutionAttempts : Integer.MAX_VALUE
        );
    }

    /**
     * @param onFailureUsage told what the requests that succeeded, and the ones that failed saying what they did, used when the compute
     *                       fails, which the failure cannot carry; see {@link ComputeListener}
     */
    final void startComputeOnDataNodes(
        Set<String> concreteIndices,
        Runnable runOnTaskFailure,
        Consumer<DriverCompletionInfo> onFailureUsage,
        ActionListener<ComputeResponse> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(
            ThreadPool.Names.SYSTEM_READ,
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_COORDINATION,
            // Reached downstream of ComputeService.execute(), which itself may be invoked on the external blob-store
            // pool after ExternalSourceResolver completes on that pool.
            EsqlPlugin.externalBlobStorePool()
        );
        final long startTimeInNanos = System.nanoTime();
        searchShards(concreteIndices, ActionListener.wrap(targetShards -> {
            try (var computeListener = new ComputeListener(runOnTaskFailure, listener.map(completionInfo -> {
                final int totalSkipShards = targetShards.skippedShards() + skippedShards.get();
                final int failedShards = shardFailures.size();
                final int successfulShards = targetShards.totalShards() - totalSkipShards - failedShards;
                return new ComputeResponse(
                    completionInfo,
                    timeValueNanos(System.nanoTime() - startTimeInNanos),
                    targetShards.totalShards(),
                    successfulShards,
                    totalSkipShards,
                    failedShards,
                    selectFailures()
                );
            }), onFailureUsage)) {
                pendingShardIds.addAll(order(targetShards));
                trySendingRequestsForPendingShards(targetShards, computeListener);
            }
        }, listener::onFailure));
    }

    private static List<ShardId> order(TargetShards targetShards) {
        var computedNodeOrder = new HashMap<DiscoveryNode, Integer>();
        var ordered = new ArrayList<>(targetShards.shards.keySet());
        ordered.sort(Comparator.comparingInt(shardId -> nodesOrder(targetShards.getShard(shardId).remainingNodes, computedNodeOrder)));
        return ordered;
    }

    private static int nodesOrder(List<DiscoveryNode> nodes, Map<DiscoveryNode, Integer> computedNodeOrder) {
        if (nodes.isEmpty()) {
            return Integer.MAX_VALUE;
        }
        var order = 0;
        for (var node : nodes) {
            order = Math.max(order, computedNodeOrder.computeIfAbsent(node, DataNodeRequestSender::nodeOrder));
        }
        return order;
    }

    private static int nodeOrder(DiscoveryNode node) {
        for (int i = 0; i < NODE_QUERY_ORDER.size(); i++) {
            if (node.hasRole(NODE_QUERY_ORDER.get(i))) {
                return i;
            }
        }
        return Integer.MAX_VALUE;
    }

    private void trySendingRequestsForPendingShards(TargetShards targetShards, ComputeListener computeListener) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH)
            || (rootTask.isCancelled() && Transports.isTransportThread(Thread.currentThread()));
        changed.set(true);
        final ActionListener<Void> listener = computeListener.acquireAvoid();
        try {
            while (sendingLock.tryLock()) {
                try {
                    if (changed.compareAndSet(true, false)) {
                        applyEndedWaits();
                        var pendingRetries = new HashSet<ShardId>();
                        for (ShardId shardId : pendingShardIds) {
                            if (waitingShards.contains(shardId) == false && targetShards.getShard(shardId).remainingNodes.isEmpty()) {
                                if (isRetryableFailure(shardFailures.get(shardId))) {
                                    pendingRetries.add(shardId);
                                }
                            }
                        }
                        Set<ShardId> parkedThisPass = new HashSet<>();
                        ResolvedShards resolved = null;
                        boolean counted = false;
                        final boolean cancelled = rootTask.isCancelled();
                        final boolean finished = cancelled == false && isQueryFinished();
                        if (cancelled || finished) {
                            // Apply the reading only to the retry candidates. Shards still holding a node go to sendRequest
                            // and are handled via onSkip, as on main; a fatal failure recorded elsewhere is not turned into a
                            // skip here.
                            for (ShardId shardId : pendingRetries) {
                                if (cancelled) {
                                    // The shard ends as cancelled, as a parked one does in applyEndedWaits. Its entry is removed
                                    // first: tracking a cancellation on top of it would keep the earlier exception.
                                    shardFailures.remove(shardId);
                                    trackShardLevelFailure(
                                        shardId,
                                        true,
                                        new TaskCancelledException(rootTask.getReasonCancelled()),
                                        null,
                                        NodeFailure.OTHER
                                    );
                                } else {
                                    skipShard(shardId);
                                }
                            }
                        } else if (pendingRetries.isEmpty() == false) {
                            final boolean budgetLeft = remainingUnavailableShardResolutionAttempts.get() > 0;
                            Set<ShardId> shardsToResolve = new HashSet<>();
                            for (ShardId shardId : pendingRetries) {
                                ShardFailure failure = shardFailures.get(shardId);
                                if (routingWaitTimeout.millis() != 0 || budgetLeft || failure.lastRefused || stallExpired(shardId)) {
                                    shardsToResolve.add(shardId);
                                } else {
                                    countAttemptsExhausted();
                                }
                            }
                            if (shardsToResolve.isEmpty() == false) {
                                resolved = resolveShards(shardsToResolve);
                            }
                            final long now = threadPool.relativeTimeInMillis();
                            for (ShardId shardId : shardsToResolve) {
                                ShardFailure failure = shardFailures.get(shardId);
                                List<DiscoveryNode> remainingNodes = targetShards.getShard(shardId).remainingNodes;
                                List<ShardCopy> copies = resolved.copies.get(shardId);
                                if (routingWaitTimeout.millis() == 0) {
                                    List<DiscoveryNode> nodes = eligibleActiveCopies(copies);
                                    if (nodes.isEmpty() == false && (failure.lastRefused || budgetLeft)) {
                                        remainingNodes.addAll(nodes);
                                        counted |= failure.lastRefused == false;
                                    } else if (nodes.isEmpty() == false && failure.lastRefused == false) {
                                        countAttemptsExhausted();
                                    }
                                    continue;
                                }
                                if (clusterServiceClosed) {
                                    continue;
                                }
                                if (stallExpired(shardId)) {
                                    remainingNodes.addAll(eligibleActiveCopies(copies));
                                    stallDeadlines.remove(shardId);
                                    shardFailures.computeIfPresent(
                                        shardId,
                                        (ignored, current) -> new ShardFailure(current.fatal, false, current.lastRefused, current.failure)
                                    );
                                    continue;
                                }
                                RoutingAnswer answer = untriedCopies(copies, triedCopies.getOrDefault(shardId, Map.of()), refusedNodes);
                                switch (answer.kind) {
                                    case COPY -> {
                                        if (failure.lastRefused || budgetLeft) {
                                            remainingNodes.addAll(answer.nodes);
                                            stallDeadlines.remove(shardId);
                                            counted |= failure.lastRefused == false;
                                        } else {
                                            countAttemptsExhausted();
                                        }
                                    }
                                    case NONE_YET -> {
                                        if (failure.lastRefused || budgetLeft) {
                                            waitingShards.add(shardId);
                                            parkedThisPass.add(shardId);
                                            stallDeadlines.putIfAbsent(shardId, now + routingWaitTimeout.millis());
                                        } else {
                                            countAttemptsExhausted();
                                        }
                                    }
                                    case GONE -> stallDeadlines.remove(shardId);
                                }
                            }
                            if (counted) {
                                remainingUnavailableShardResolutionAttempts.decrementAndGet();
                            }
                        }
                        // Once the failures were reported their entries are gone, while the shards stay pending. A pass that runs
                        // after that, for the outcome of a wait or a late response, must not read a missing entry as a shard
                        // without copies and report that on top of the failure the shard really had.
                        if (reportedFailure == false) {
                            for (ShardId shardId : pendingShardIds) {
                                if (targetShards.getShard(shardId).remainingNodes.isEmpty()
                                    && waitingShards.contains(shardId) == false
                                    && (isRetryableFailure(shardFailures.get(shardId)) == false || pendingRetries.contains(shardId))) {
                                    shardFailures.compute(
                                        shardId,
                                        (k, v) -> new ShardFailure(
                                            true,
                                            false,
                                            v != null && v.lastRefused,
                                            v == null ? new NoShardAvailableActionException(shardId, "no shard copies found") : v.failure
                                        )
                                    );
                                }
                            }
                        }
                        if (reportedFailure
                            || (allowPartialResults == false
                                && shardFailures.values().stream().anyMatch(shardFailure -> shardFailure.fatal))) {
                            reportedFailure = true;
                            reportFailures(computeListener);
                            RoutingWait queuedWait;
                            while ((queuedWait = waitsToStart.poll()) != null) {
                                queuedWait.end(WaitOutcome.QUERY_FAILED);
                            }
                            for (RoutingWait wait : List.copyOf(activeWaits)) {
                                wait.end(WaitOutcome.QUERY_FAILED);
                            }
                        } else {
                            if (parkedThisPass.isEmpty() == false) {
                                long deadline = parkedThisPass.stream().mapToLong(stallDeadlines::get).min().orElseThrow();
                                waitsToStart.add(
                                    new RoutingWait(
                                        resolved.state,
                                        parkedThisPass,
                                        deadline,
                                        computeListener.acquireAvoid(),
                                        targetShards,
                                        computeListener
                                    )
                                );
                            }
                            for (NodeRequest request : selectNodeRequests(targetShards)) {
                                sendOneNodeRequest(targetShards, computeListener, request);
                            }
                        }
                    }
                } finally {
                    sendingLock.unlock();
                }
                if (sendingLock.isHeldByCurrentThread() == false) {
                    RoutingWait wait;
                    while ((wait = waitsToStart.poll()) != null) {
                        wait.start();
                    }
                }
                // recheck after releasing the lock as another response might have set changed, but failed to acquire the lock
                if (changed.get() == false) {
                    break;
                }
            }
        } finally {
            listener.onResponse(null);
        }
    }

    private void applyEndedWaits() {
        assert sendingLock.isHeldByCurrentThread();
        final boolean cancelled = rootTask.isCancelled();
        final boolean finished = cancelled == false && isQueryFinished();
        EndedWait ended;
        while ((ended = endedWaits.poll()) != null) {
            WaitOutcome outcome = cancelled ? WaitOutcome.CANCELLED : finished ? WaitOutcome.QUERY_FINISHED : ended.outcome;
            RoutingWait endedRoutingWait = ended.routingWait;
            for (ShardId shardId : endedRoutingWait.shards) {
                waitingShards.remove(shardId);
                switch (outcome) {
                    case ROUTING_CHANGED -> {
                    }
                    case TIMED_OUT -> stallDeadlines.computeIfPresent(
                        shardId,
                        (ignored, deadline) -> deadline <= endedRoutingWait.deadlineMillis ? STALL_EXPIRED : deadline
                    );
                    case CLUSTER_SERVICE_CLOSED -> clusterServiceClosed = true;
                    case CANCELLED -> {
                        shardFailures.remove(shardId);
                        trackShardLevelFailure(
                            shardId,
                            true,
                            new TaskCancelledException(rootTask.getReasonCancelled()),
                            null,
                            NodeFailure.OTHER
                        );
                    }
                    case QUERY_FINISHED -> skipShard(shardId);
                    case QUERY_FAILED -> {
                        pendingShardIds.remove(shardId);
                        stallDeadlines.remove(shardId);
                    }
                }
            }
        }
    }

    /** Whether the wait of the shard timed out, so that the shard is due its last try. */
    private boolean stallExpired(ShardId shardId) {
        assert sendingLock.isHeldByCurrentThread();
        Long deadline = stallDeadlines.get(shardId);
        return deadline != null && deadline == STALL_EXPIRED;
    }

    private void skipShard(ShardId shardId) {
        if (pendingShardIds.remove(shardId)) {
            skippedShards.incrementAndGet();
        }
        waitingShards.remove(shardId);
        shardFailures.remove(shardId);
        stallDeadlines.remove(shardId);
    }

    private void countAttemptsExhausted() {
        if (attemptsExhaustedCounted.compareAndSet(false, true)) {
            metrics.attemptsExhausted();
        }
    }

    private void reportFailures(ComputeListener computeListener) {
        assert sendingLock.isHeldByCurrentThread();
        assert reportedFailure;
        Iterator<ShardFailure> it = shardFailures.values().iterator();
        Set<Exception> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        while (it.hasNext()) {
            ShardFailure failure = it.next();
            if (seen.add(failure.failure)) {
                computeListener.acquireAvoid().onFailure(failure.failure);
            }
            it.remove();
        }
    }

    private List<ShardSearchFailure> selectFailures() {
        assert reportedFailure == false;
        final List<ShardSearchFailure> failures = new ArrayList<>();
        final Set<Exception> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Map.Entry<ShardId, ShardFailure> e : shardFailures.entrySet()) {
            final ShardFailure failure = e.getValue();
            if (ExceptionsHelper.unwrap(failure.failure(), TaskCancelledException.class) != null) {
                continue;
            }
            if (seen.add(failure.failure) && failures.size() < 5) {
                failures.add(new ShardSearchFailure(failure.failure, new SearchShardTarget(null, e.getKey(), clusterAlias)));
            }
        }
        // pick any cancellation exception
        if (failures.isEmpty() && shardFailures.isEmpty() == false) {
            final ShardFailure any = shardFailures.values().iterator().next();
            failures.add(new ShardSearchFailure(any.failure));
        }
        return failures;
    }

    private void sendOneNodeRequest(TargetShards targetShards, ComputeListener computeListener, NodeRequest request) {
        final ClusterState applied = clusterService.state();
        final Map<ShardId, SentCopy> sent = new HashMap<>();
        for (DataNodeRequest.Shard shard : request.shards) {
            SentCopy copy = sentCopy(applied, shard.shardId(), request.node);
            if (copy != null) {
                sent.put(shard.shardId(), copy);
            }
        }
        final ActionListener<DriverCompletionInfo> listener = computeListener.acquireCompute();
        sendRequest(request.node, request.shards, request.aliasFilters, new NodeListener() {

            void onAfterRequest() {
                nodePermits.get(request.node).release();
                if (concurrentRequests != null) {
                    concurrentRequests.release();
                }
                trySendingRequestsForPendingShards(targetShards, computeListener);
            }

            @Override
            public void onResponse(DataNodeComputeResponse response) {
                try {
                    // remove failures of successful shards
                    for (var shard : request.shards) {
                        ShardId shardId = shard.shardId();
                        if (response.shardLevelFailures().containsKey(shardId) == false) {
                            shardFailures.remove(shardId);
                        }
                    }
                    for (var entry : response.shardLevelFailures().entrySet()) {
                        final ShardId shardId = entry.getKey();
                        trackShardLevelFailure(shardId, false, entry.getValue(), sent.get(shardId), NodeFailure.OTHER);
                        pendingShardIds.add(shardId);
                    }
                    onAfterRequest();
                } catch (Exception ex) {
                    expectNoFailure("expect no failure while handling data node response", ex);
                    listener.onFailure(ex);
                    return;
                }
                listener.onResponse(response.completionInfo());
            }

            @Override
            public void onFailure(Exception e, boolean receivedData, NodeFailure nodeFailure) {
                try {
                    if (nodeFailure == NodeFailure.REFUSED) {
                        refusedNodes.add(request.node);
                    }
                    for (var shard : request.shards) {
                        ShardId shardId = shard.shardId();
                        trackShardLevelFailure(shardId, receivedData, e, sent.get(shardId), nodeFailure);
                        pendingShardIds.add(shardId);
                    }
                    onAfterRequest();
                } catch (Exception ex) {
                    expectNoFailure("expect no failure while handling failure of data node request", ex);
                    listener.onFailure(ex);
                    return;
                }
                listener.onResponse(DriverCompletionInfo.EMPTY);
            }

            @Override
            public void onSkip() {
                skippedShards.incrementAndGet();
                if (rootTask.isCancelled()) {
                    onFailure(new TaskCancelledException("null"), true);
                } else {
                    onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of()));
                }
            }

            private void expectNoFailure(String message, Exception e) {
                LOGGER.error(message, e);
                assert false : new AssertionError(message, e);
            }
        });
    }

    abstract void sendRequest(
        DiscoveryNode node,
        List<DataNodeRequest.Shard> shards,
        Map<Index, AliasFilter> aliasFilters,
        NodeListener nodeListener
    );

    interface NodeListener {
        void onResponse(DataNodeComputeResponse response);

        default void onFailure(Exception e, boolean receivedData) {
            onFailure(e, receivedData, classifyNodeFailure(e));
        }

        void onFailure(Exception e, boolean receivedData, NodeFailure nodeFailure);

        void onSkip();
    }

    /** What a failed transport request says about the node it targeted. */
    enum NodeFailure {
        REFUSED,
        UNREACHABLE,
        OTHER
    }

    static NodeFailure classifyNodeFailure(Exception e) {
        if (e instanceof RemoteTransportException) {
            return ExceptionsHelper.unwrapCause(e) instanceof NodeClosedException ? NodeFailure.REFUSED : NodeFailure.OTHER;
        }
        if (e instanceof ConnectTransportException
            || (e instanceof SendRequestTransportException && e.getCause() instanceof ConnectTransportException)) {
            return NodeFailure.UNREACHABLE;
        }
        return NodeFailure.OTHER;
    }

    /**
     * The admission gate only refuses opens, so a refusal reported by the data request came from further downstream.
     */
    static NodeFailure classifyDataRequestFailure(Exception e) {
        NodeFailure nodeFailure = classifyNodeFailure(e);
        return nodeFailure == NodeFailure.REFUSED ? NodeFailure.OTHER : nodeFailure;
    }

    private static Exception unwrapFailure(ShardId shardId, Exception e) {
        e = e instanceof TransportException te ? FailureCollector.unwrapTransportException(te) : e;
        if (TransportActions.isShardNotAvailableException(e)) {
            var ex = NoShardAvailableActionException.forOnShardFailureWrapper(e.getMessage());
            ex.setShard(shardId);
            return ex;
        } else {
            return e;
        }
    }

    private void trackShardLevelFailure(
        ShardId shardId,
        boolean fatal,
        Exception originalEx,
        @Nullable SentCopy sentCopy,
        NodeFailure nodeFailure
    ) {
        final Exception e = unwrapFailure(shardId, originalEx);
        final boolean isTaskCanceledException = ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null;
        final boolean isCircuitBreakerException = ExceptionsHelper.unwrap(e, CircuitBreakingException.class) != null;
        final boolean resolvable = switch (nodeFailure) {
            case REFUSED, UNREACHABLE -> true;
            case OTHER -> e instanceof NoShardAvailableActionException;
        };
        if (resolvable && nodeFailure != NodeFailure.REFUSED && sentCopy != null) {
            triedCopies.computeIfAbsent(shardId, ignored -> ConcurrentCollections.newConcurrentMap())
                .merge(sentCopy.allocationId, sentCopy.active, Boolean::logicalOr);
        }
        shardFailures.compute(shardId, (k, current) -> {
            boolean mergedFatal = fatal || isTaskCanceledException || isCircuitBreakerException;
            return current == null
                ? new ShardFailure(mergedFatal, resolvable, nodeFailure == NodeFailure.REFUSED, e)
                : new ShardFailure(
                    mergedFatal || current.fatal,
                    resolvable && current.resolvable,
                    nodeFailure == NodeFailure.REFUSED,
                    // Retain only one meaningful exception and avoid suppressing previous failures to minimize memory usage,
                    // especially when handling many shards.
                    isTaskCanceledException || resolvable ? current.failure : e
                );
        });
    }

    /**
     * Result from {@link #searchShards(Set, ActionListener)} where can_match is performed to
     * determine what shards can be skipped and which target nodes are needed for running the ES|QL query
     *
     * @param shards        List of target shards to perform the ES|QL query on
     * @param totalShards   Total number of shards (from can_match phase), including skipped shards
     * @param skippedShards Number of skipped shards (from can_match phase)
     */
    record TargetShards(Map<ShardId, TargetShard> shards, int totalShards, int skippedShards) {
        TargetShard getShard(ShardId shardId) {
            return shards.get(shardId);
        }
    }

    /**
     * Information required to send requests for a shard with this shardId.
     * Note that {@link SplitShardCountSummary} value should never change, it is important for resharding feature to work.
     */
    record TargetShard(
        ShardId shardId,
        List<DiscoveryNode> remainingNodes,
        AliasFilter aliasFilter,
        SplitShardCountSummary splitShardCountSummary
    ) {}

    record NodeRequest(DiscoveryNode node, List<DataNodeRequest.Shard> shards, Map<Index, AliasFilter> aliasFilters) {}

    private record ShardFailure(boolean fatal, boolean resolvable, boolean lastRefused, Exception failure) {}

    private static boolean isRetryableFailure(ShardFailure failure) {
        return failure != null && failure.fatal == false && failure.resolvable;
    }

    /**
     * What the routing says about a searchable copy, which decides whether a retry may go to it.
     */
    enum CopyState {
        /** Started, or relocating away: the copy the routing sends searches to. */
        ACTIVE,
        /**
         * The copy a relocating one is moving to. It is a candidate although it has not started: by the time the source answers
         * "not available" the hand-off is often done, and the target answers before the state that shows it started is applied here.
         */
        RELOCATION_TARGET,
        /**
         * Initializing and not the target of a relocation. Never a candidate. It is listed so that a shard that has only such
         * copies reads as "none yet", and waits for one to start, and not as "gone".
         */
        INITIALIZING
    }

    /**
     * One searchable copy as the routing lists it.
     *
     * @param allocationId {@link AllocationId#getId}, which a relocation target keeps when it starts. It tells a copy that was
     *                     tried from a new one on the same node.
     */
    record ShardCopy(DiscoveryNode node, String allocationId, CopyState state) {}

    /**
     * The copies of the shards that were resolved, with the state they were read from. A routing wait observes from that state on:
     * a state applied between the resolution and the start of the wait is newer than it, and ends the wait.
     *
     * @param copies no entry for a shard whose index or shard table the state does not have
     */
    record ResolvedShards(ClusterState state, Map<ShardId, List<ShardCopy>> copies) {}

    /**
     * The copy a request was sent to, as the applied state showed it when the request was sent. It is recorded then and read when
     * the request fails, since by that time the routing may show another copy on the node, or the same one started.
     *
     * @param active whether the routing showed the copy active; a copy that failed while it was initializing is tried again once
     *               it is active
     */
    record SentCopy(String allocationId, boolean active) {}

    /**
     * What a routing table says about a shard whose known copies have all failed as unavailable.
     */
    enum Untried {
        /** There is a copy to try. */
        COPY,
        /** The shard is there, and every copy that could be tried has been: newer routing may bring one, so the shard can wait. */
        NONE_YET,
        /** The routing has no copy of the shard at all, or not the shard: nothing to wait for. */
        GONE
    }

    /**
     * @param nodes for {@link Untried#COPY}: the nodes of the untried copies, active ones first, then relocation targets; empty
     *              otherwise
     */
    record RoutingAnswer(Untried kind, List<DiscoveryNode> nodes) {}

    /**
     * Compares the copies the routing lists for a shard with the ones that were tried. Static and total, since it also runs in the
     * predicate of a routing wait on the cluster applier thread, where it must neither throw nor read anything of the pass.
     *
     * @param copies  the copies of the shard as {@link #resolve} lists them, or {@code null} when the routing does not have the shard
     * @param tried   the copies that failed as unavailable, see {@link #triedCopies}
     * @param refused the nodes that refused a request; none of their copies is tried again
     */
    static RoutingAnswer untriedCopies(@Nullable List<ShardCopy> copies, Map<String, Boolean> tried, Set<DiscoveryNode> refused) {
        if (copies == null || copies.isEmpty()) {
            return new RoutingAnswer(Untried.GONE, List.of());
        }
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (ShardCopy copy : copies) {
            if (copy.state == CopyState.INITIALIZING || refused.contains(copy.node)) {
                continue;
            }
            Boolean activeWhenTried = tried.get(copy.allocationId);
            if (activeWhenTried == null || (activeWhenTried == false && copy.state == CopyState.ACTIVE)) {
                nodes.add(copy.node);
            }
        }
        return nodes.isEmpty() ? new RoutingAnswer(Untried.NONE_YET, List.of()) : new RoutingAnswer(Untried.COPY, nodes);
    }

    private List<DiscoveryNode> eligibleActiveCopies(@Nullable List<ShardCopy> copies) {
        if (copies == null) {
            return List.of();
        }
        return copies.stream()
            .filter(copy -> copy.state == CopyState.ACTIVE && refusedNodes.contains(copy.node) == false)
            .map(ShardCopy::node)
            .toList();
    }

    /**
     * Selects the next nodes to send requests to. Limits to at most one outstanding request per node.
     * If there is already a request in-flight to a node, another request will not be sent to the same node
     * until the first request completes. Instead, the next node in the remaining nodes will be tried.
     */
    private List<NodeRequest> selectNodeRequests(TargetShards targetShards) {
        assert sendingLock.isHeldByCurrentThread();
        final Map<DiscoveryNode, List<DataNodeRequest.Shard>> nodeToShardMetadata = new LinkedHashMap<>();
        final Iterator<ShardId> shardsIt = pendingShardIds.iterator();

        while (shardsIt.hasNext()) {
            ShardId shardId = shardsIt.next();
            ShardFailure failure = shardFailures.get(shardId);
            if (failure != null && failure.fatal) {
                shardsIt.remove();
                continue;
            }
            TargetShard shard = targetShards.getShard(shardId);
            Iterator<DiscoveryNode> nodesIt = shard.remainingNodes.iterator();
            while (nodesIt.hasNext()) {
                DiscoveryNode node = nodesIt.next();
                List<DataNodeRequest.Shard> pendingRequest = nodeToShardMetadata.get(node);
                if (pendingRequest != null) {
                    pendingRequest.add(new DataNodeRequest.Shard(shard.shardId, shard.splitShardCountSummary));
                    nodesIt.remove();
                    shardsIt.remove();
                    break;
                }

                if (concurrentRequests == null || concurrentRequests.tryAcquire()) {
                    if (nodePermits.computeIfAbsent(node, n -> new Semaphore(1)).tryAcquire()) {
                        pendingRequest = new ArrayList<>();
                        pendingRequest.add(new DataNodeRequest.Shard(shard.shardId, shard.splitShardCountSummary));
                        nodeToShardMetadata.put(node, pendingRequest);

                        nodesIt.remove();
                        shardsIt.remove();

                        break;
                    } else if (concurrentRequests != null) {
                        concurrentRequests.release();
                    }
                }
            }
        }

        final List<NodeRequest> nodeRequests = new ArrayList<>(nodeToShardMetadata.size());
        for (var entry : nodeToShardMetadata.entrySet()) {
            var node = entry.getKey();
            var shards = entry.getValue();
            Map<Index, AliasFilter> aliasFilters = new HashMap<>();
            for (DataNodeRequest.Shard shard : shards) {
                var aliasFilter = targetShards.getShard(shard.shardId()).aliasFilter;
                if (aliasFilter != null) {
                    aliasFilters.put(shard.shardId().getIndex(), aliasFilter);
                }
            }
            nodeRequests.add(new NodeRequest(node, shards, aliasFilters));
        }
        return nodeRequests;
    }

    /**
     * Performs can_match and find the target nodes for the given target indices and filter.
     * <p>
     * Ideally, the search_shards API should be called before the field-caps API; however, this can lead
     * to a situation where the column structure (i.e., matched data types) differs depending on the query.
     */
    void searchShards(Set<String> concreteIndices, ActionListener<TargetShards> listener) {
        ActionListener<SearchShardsResponse> searchShardsListener = listener.map(resp -> {
            Map<String, DiscoveryNode> nodes = newHashMap(resp.getNodes().size());
            for (DiscoveryNode node : resp.getNodes()) {
                nodes.put(node.getId(), node);
            }
            int totalShards = resp.getNumSkippedShards();
            int skippedShards = resp.getNumSkippedShards();
            Map<ShardId, TargetShard> shards = newHashMap(resp.getGroups().size());
            for (SearchShardsGroup group : resp.getGroups()) {
                var shardId = group.shardId();
                if (concreteIndices.contains(shardId.getIndexName()) == false) {
                    continue;
                }
                totalShards++;
                if (group.skipped()) {
                    skippedShards++;
                    continue;
                }
                List<DiscoveryNode> allocatedNodes = new ArrayList<>(group.allocatedNodes().size());
                for (String n : group.allocatedNodes()) {
                    allocatedNodes.add(nodes.get(n));
                }
                AliasFilter aliasFilter = resp.getAliasFilters().get(shardId.getIndex().getUUID());
                shards.put(shardId, new TargetShard(shardId, allocatedNodes, aliasFilter, group.splitShardCountSummary()));
            }
            return new TargetShards(shards, totalShards, skippedShards);
        });
        transportService.sendChildRequest(
            transportService.getLocalNode(),
            EsqlSearchShardsAction.TYPE.name(),
            searchShardsRequest(anySliceEnabled(concreteIndices)),
            rootTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(searchShardsListener, SearchShardsResponse::new, searchExecutor)
        );
    }

    /**
     * The request that resolves the shards to query. A slice is the routing value of its documents, so the slices only
     * target the shards that hold them. Shard resolution rejects slices when no index is slice-enabled, so they are left
     * out then: the filter on {@code _slice} matches nothing on such an index.
     */
    SearchShardsRequest searchShardsRequest(boolean anySliceEnabled) {
        final boolean routeBySlice = anySliceEnabled && slices.isRestricted();
        return new SearchShardsRequest(
            originalIndices.indices(),
            originalIndices.indicesOptions(),
            requestFilter,
            routeBySlice ? slices.toRouting() : null,
            routeBySlice,
            null,
            true, // unavailable_shards will be handled by the sender
            clusterAlias
        );
    }

    /**
     * Whether any of the indices is slice-enabled.
     */
    private boolean anySliceEnabled(Set<String> concreteIndices) {
        if (slices.isRestricted() == false) {
            return false;
        }
        var project = projectResolver.getProjectMetadata(clusterService.state());
        for (String concreteIndex : concreteIndices) {
            var indexMetadata = project.index(concreteIndex);
            if (indexMetadata != null && IndexSettings.SLICE_ENABLED.get(indexMetadata.getSettings())) {
                return true;
            }
        }
        return false;
    }

    ResolvedShards resolveShards(Set<ShardId> shardIds) {
        ClusterState state = clusterService.state();
        return new ResolvedShards(state, resolve(state, projectId, shardIds));
    }

    /**
     * Resolves without throwing or consulting thread context, since routing-wait predicates run on the cluster applier thread.
     */
    static Map<ShardId, List<ShardCopy>> resolve(ClusterState state, ProjectId projectId, Set<ShardId> shardIds) {
        RoutingTable routingTable = state.globalRoutingTable().routingTables().get(projectId);
        if (routingTable == null) {
            return Map.of();
        }
        Map<ShardId, List<ShardCopy>> result = new HashMap<>();
        for (ShardId shardId : shardIds) {
            IndexRoutingTable index = routingTable.index(shardId.getIndex());
            if (index == null) {
                continue;
            }
            IndexShardRoutingTable shardTable = index.shard(shardId.id());
            if (shardTable == null) {
                continue;
            }
            List<ShardCopy> copies = new ArrayList<>();
            List<ShardRouting> routings = shardTable.allShards().toList();
            for (ShardRouting shard : routings) {
                if (shard.active() && shard.isSearchable()) {
                    addCopy(state, copies, shard, CopyState.ACTIVE);
                }
            }
            for (ShardRouting shard : routings) {
                if (shard.relocating() && shard.isSearchable()) {
                    addCopy(state, copies, shard.getTargetRelocatingShard(), CopyState.RELOCATION_TARGET);
                }
            }
            for (ShardRouting shard : routings) {
                if (shard.initializing() && shard.isSearchable()) {
                    addCopy(state, copies, shard, CopyState.INITIALIZING);
                }
            }
            result.put(shardId, copies);
        }
        return result;
    }

    private static void addCopy(ClusterState state, List<ShardCopy> copies, ShardRouting shard, CopyState copyState) {
        DiscoveryNode node = state.nodes().get(shard.currentNodeId());
        if (node != null && shard.allocationId() != null) {
            copies.add(new ShardCopy(node, shard.allocationId().getId(), copyState));
        }
    }

    /**
     * Finds which allocation was targeted using one applied state, so later routing changes cannot rewrite the send's identity.
     */
    @Nullable
    SentCopy sentCopy(ClusterState applied, ShardId shardId, DiscoveryNode node) {
        RoutingTable routingTable = applied.globalRoutingTable().routingTables().get(projectId);
        if (routingTable == null) {
            return null;
        }
        IndexRoutingTable index = routingTable.index(shardId.getIndex());
        IndexShardRoutingTable shardTable = index == null ? null : index.shard(shardId.id());
        if (shardTable == null) {
            return null;
        }
        for (ShardRouting shard : shardTable.allShards().toList()) {
            if (node.getId().equals(shard.currentNodeId()) && (shard.active() || shard.initializing())) {
                return new SentCopy(shard.allocationId().getId(), shard.active());
            }
            if (shard.relocating()) {
                ShardRouting target = shard.getTargetRelocatingShard();
                if (node.getId().equals(target.currentNodeId())) {
                    return new SentCopy(target.allocationId().getId(), false);
                }
            }
        }
        return null;
    }

    /** Whether the query has stopped consuming pages and therefore needs no more shard work. */
    abstract boolean isQueryFinished();

    /** Adds a listener for the transition described by {@link #isQueryFinished()}. */
    abstract void addQueryFinishedListener(ActionListener<Void> listener);

    private enum WaitOutcome {
        ROUTING_CHANGED,
        TIMED_OUT,
        CLUSTER_SERVICE_CLOSED,
        CANCELLED,
        QUERY_FINISHED,
        QUERY_FAILED
    }

    private record EndedWait(RoutingWait routingWait, WaitOutcome outcome) {}

    /** One bounded routing wait shared by every shard parked in a single sending pass. */
    private final class RoutingWait {
        private final ClusterState observed;
        private final Set<ShardId> shards;
        private final long deadlineMillis;
        private final ActionListener<Void> reference;
        private final Supplier<ThreadContext.StoredContext> context;
        private final TargetShards targetShards;
        private final ComputeListener computeListener;
        private final AtomicBoolean ended = new AtomicBoolean();
        private final ObserverCallback callback = new ObserverCallback(this);
        private volatile Scheduler.Cancellable timer;

        private RoutingWait(
            ClusterState observed,
            Set<ShardId> shards,
            long deadlineMillis,
            ActionListener<Void> reference,
            TargetShards targetShards,
            ComputeListener computeListener
        ) {
            this.observed = observed;
            this.shards = Set.copyOf(shards);
            this.deadlineMillis = deadlineMillis;
            this.reference = reference;
            this.targetShards = targetShards;
            this.computeListener = computeListener;
            this.context = threadPool.getThreadContext().newRestorableContext(false);
        }

        private void start() {
            // Register in activeWaits first, then read the terminating signals: the pass that reports a failure sets
            // reportedFailure before it sweeps activeWaits, so one of start() and the pass sees the other and the wait
            // does not run until its timeout while holding a reference on the compute listener.
            activeWaits.add(this);
            if (hooksRegistered.compareAndSet(false, true)) {
                rootTask.addListener(() -> endAll(WaitOutcome.CANCELLED));
                addQueryFinishedListener(ActionListener.running(() -> endAll(WaitOutcome.QUERY_FINISHED)));
            }
            if (rootTask.isCancelled()) {
                end(WaitOutcome.CANCELLED);
                return;
            }
            if (isQueryFinished()) {
                end(WaitOutcome.QUERY_FINISHED);
                return;
            }
            if (reportedFailure) {
                end(WaitOutcome.QUERY_FAILED);
                return;
            }
            final TimeValue remaining = TimeValue.timeValueMillis(Math.max(0L, deadlineMillis - threadPool.relativeTimeInMillis()));
            metrics.routingWaitStarted();
            LOGGER.debug(
                "waiting for newer shard routing for [{}] shards for up to [{}] from cluster state version [{}]",
                shards.size(),
                remaining,
                observed.version()
            );
            try {
                timer = threadPool.schedule(() -> end(WaitOutcome.TIMED_OUT), remaining, threadPool.generic());
                if (ended.get()) {
                    timer.cancel();
                }
                ClusterStateObserver observer = new ClusterStateObserver(
                    observed,
                    clusterService,
                    remaining,
                    OBSERVER_LOGGER,
                    threadPool.getThreadContext()
                );
                observer.waitForNextChange(callback, state -> {
                    RoutingWait wait = callback.wait.get();
                    return wait == null || wait.showsUntriedCopyOrGone(state);
                });
            } catch (Exception e) {
                // The catch is broad; without this line a bug in the start of the wait looks like a closing cluster service.
                LOGGER.debug("failed to start routing wait, treating as cluster service close", e);
                end(WaitOutcome.CLUSTER_SERVICE_CLOSED);
            }
        }

        private boolean showsUntriedCopyOrGone(ClusterState state) {
            Map<ShardId, List<ShardCopy>> copies = resolve(state, projectId, shards);
            for (ShardId shardId : shards) {
                RoutingAnswer answer = untriedCopies(copies.get(shardId), triedCopies.getOrDefault(shardId, Map.of()), refusedNodes);
                if (answer.kind != Untried.NONE_YET) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Ends the wait, once, and hands its outcome to the sending loop on the search pool. Called from any thread: the cluster
         * applier, the timer, the thread that cancels the task or the one that finishes the exchange source.
         * <p>
         * The hand-off is forced onto the queue, so only a search pool that is shutting down rejects it, and then the coordinator
         * itself is closing. The loop will not run again for the shards of this wait, so the wait fails its own reference to the
         * compute listener of the sender: with the failure one of its shards had before it waited, or with the rejection if none
         * is left. What follows was traced through the callers of the local cluster, {@code ComputeService#executePlan}:
         * <ol>
         * <li>The failed reference first runs the cancel-on-failure hook of the sender's {@link ComputeListener}, which is
         * {@code ComputeService#cancelQueryOnFailure} for the task of the query. That task is cancelled, on this thread, before
         * the failure goes any further.</li>
         * <li>Nothing else holds the compute listener for the shards of this wait, so the listener of the sender completes, and
         * with a failure, never with a {@link ComputeResponse}. A cancellation collected next to another failure is dropped, so
         * the failure is the shard's.</li>
         * <li>Without partial results {@code ComputeService} passes that failure on, and the request fails with it.</li>
         * <li>With partial results {@code ComputeService} accepts the failure ({@code EsqlCCSUtils#canAllowPartial}), marks the
         * local cluster {@code PARTIAL} with the failure recorded and lets the rest of the compute go on. That does not become a
         * response, because step 1 has cancelled the task: either a driver of the coordinator fails on the cancellation, or the
         * compute completes and {@code TransportEsqlQueryAction#failIfCancelled} turns its result into a failure. The request
         * fails with a {@link TaskCancelledException} that says "cancelled on failure", and not with the shard's failure.</li>
         * </ol>
         * So the client gets a failure in both modes: no success without the failure, and no partial result either.
         * <p>
         * The completion of the coordinator's own driver is handed to the search pool as well ({@code DriverTaskRunner}). A pool
         * that rejects this hand-off rejects that one too, and the transport layer then completes it on the rejecting thread.
         */
        private void end(WaitOutcome outcome) {
            if (ended.compareAndSet(false, true) == false) {
                return;
            }
            activeWaits.remove(this);
            callback.wait.set(null);
            Scheduler.Cancellable currentTimer = timer;
            if (currentTimer != null) {
                currentTimer.cancel();
            }
            if (outcome == WaitOutcome.TIMED_OUT) {
                metrics.routingWaitTimedOut();
            }
            AtomicReference<Exception> handoffFailure = new AtomicReference<>();
            searchExecutor.execute(new AbstractRunnable() {
                @Override
                public boolean isForceExecution() {
                    return true;
                }

                @Override
                protected void doRun() {
                    try (ThreadContext.StoredContext ignored = context.get()) {
                        endedWaits.add(new EndedWait(RoutingWait.this, outcome));
                        trySendingRequestsForPendingShards(targetShards, computeListener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    handoffFailure.compareAndSet(null, e);
                }

                @Override
                public void onAfter() {
                    Exception failure = handoffFailure.get();
                    if (failure == null) {
                        reference.onResponse(null);
                        return;
                    }
                    Exception retained = shards.stream()
                        .map(shardFailures::get)
                        .filter(Objects::nonNull)
                        .map(ShardFailure::failure)
                        .findFirst()
                        .orElse(failure);
                    reference.onFailure(retained);
                }
            });
        }
    }

    private void endAll(WaitOutcome outcome) {
        for (RoutingWait wait : List.copyOf(activeWaits)) {
            wait.end(outcome);
        }
    }

    /** The observer may outlive its wait, so ending the wait clears the only reference to its state. */
    private static final class ObserverCallback implements ClusterStateObserver.Listener {
        private final AtomicReference<RoutingWait> wait;

        private ObserverCallback(RoutingWait wait) {
            this.wait = new AtomicReference<>(wait);
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            RoutingWait current = wait.get();
            if (current != null) {
                current.end(WaitOutcome.ROUTING_CHANGED);
            }
        }

        @Override
        public void onClusterServiceClose() {
            RoutingWait current = wait.get();
            if (current != null) {
                current.end(WaitOutcome.CLUSTER_SERVICE_CLOSED);
            }
        }

        @Override
        public void onTimeout(TimeValue elapsed) {
            RoutingWait current = wait.get();
            if (current != null) {
                current.end(WaitOutcome.TIMED_OUT);
            }
        }
    }
}
