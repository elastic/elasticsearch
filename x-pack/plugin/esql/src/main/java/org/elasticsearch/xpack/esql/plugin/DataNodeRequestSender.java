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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.core.TimeValue.timeValueNanos;

/**
 * Handles computes within a single cluster by dispatching {@link DataNodeRequest} to data nodes
 * and executing these computes on the data nodes.
 */
abstract class DataNodeRequestSender {

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
    private final TransportService transportService;
    private final Executor esqlExecutor;
    private final CancellableTask rootTask;

    private final String clusterAlias;
    private final OriginalIndices originalIndices;
    private final QueryBuilder requestFilter;

    private final boolean allowPartialResults;
    private final Semaphore concurrentRequests;
    private final ReentrantLock sendingLock = new ReentrantLock();
    private final Queue<ShardId> pendingShardIds = ConcurrentCollections.newQueue();
    private final Map<DiscoveryNode, Semaphore> nodePermits = new HashMap<>();
    private final Map<ShardId, ShardFailure> shardFailures = ConcurrentCollections.newConcurrentMap();
    private final AtomicInteger skippedShards = new AtomicInteger();
    private final AtomicBoolean changed = new AtomicBoolean();
    private boolean reportedFailure = false; // guarded by sendingLock
    private final AtomicInteger remainingUnavailableShardResolutionAttempts;

    DataNodeRequestSender(
        ClusterService clusterService,
        TransportService transportService,
        Executor esqlExecutor,
        CancellableTask rootTask,
        OriginalIndices originalIndices,
        QueryBuilder requestFilter,
        String clusterAlias,
        boolean allowPartialResults,
        int concurrentRequests,
        int unavailableShardResolutionAttempts
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.esqlExecutor = esqlExecutor;
        this.rootTask = rootTask;
        this.originalIndices = originalIndices;
        this.requestFilter = requestFilter;
        this.clusterAlias = clusterAlias;
        this.allowPartialResults = allowPartialResults;
        this.concurrentRequests = concurrentRequests > 0 ? new Semaphore(concurrentRequests) : null;
        this.remainingUnavailableShardResolutionAttempts = new AtomicInteger(
            unavailableShardResolutionAttempts >= 0 ? unavailableShardResolutionAttempts : Integer.MAX_VALUE
        );
    }

    final void startComputeOnDataNodes(Set<String> concreteIndices, Runnable runOnTaskFailure, ActionListener<ComputeResponse> listener) {
        final long startTimeInNanos = System.nanoTime();
        searchShards(concreteIndices, ActionListener.wrap(targetShards -> {
            try (
                var computeListener = new ComputeListener(
                    transportService.getThreadPool(),
                    runOnTaskFailure,
                    listener.map(
                        profiles -> new ComputeResponse(
                            profiles,
                            timeValueNanos(System.nanoTime() - startTimeInNanos),
                            targetShards.totalShards(),
                            targetShards.totalShards() - shardFailures.size() - skippedShards.get(),
                            targetShards.skippedShards() + skippedShards.get(),
                            shardFailures.size(),
                            selectFailures()
                        )
                    )
                )
            ) {
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
        changed.set(true);
        final ActionListener<Void> listener = computeListener.acquireAvoid();
        try {
            while (sendingLock.tryLock()) {
                try {
                    if (changed.compareAndSet(true, false) == false) {
                        break;
                    }
                    var pendingRetries = new HashSet<ShardId>();
                    for (ShardId shardId : pendingShardIds) {
                        if (targetShards.getShard(shardId).remainingNodes.isEmpty()) {
                            if (isRetryableFailure(shardFailures.get(shardId))) {
                                pendingRetries.add(shardId);
                            }
                        }
                    }
                    if (pendingRetries.isEmpty() == false && remainingUnavailableShardResolutionAttempts.decrementAndGet() >= 0) {
                        for (var entry : resolveShards(pendingRetries).entrySet()) {
                            targetShards.getShard(entry.getKey()).remainingNodes.addAll(entry.getValue());
                        }
                    }
                    for (ShardId shardId : pendingShardIds) {
                        if (targetShards.getShard(shardId).remainingNodes.isEmpty()
                            && (isRetryableFailure(shardFailures.get(shardId)) == false || pendingRetries.contains(shardId))) {
                            shardFailures.compute(
                                shardId,
                                (k, v) -> new ShardFailure(
                                    true,
                                    v == null ? new NoShardAvailableActionException(shardId, "no shard copies found") : v.failure
                                )
                            );
                        }
                    }
                    if (reportedFailure
                        || (allowPartialResults == false && shardFailures.values().stream().anyMatch(shardFailure -> shardFailure.fatal))) {
                        reportedFailure = true;
                        reportFailures(computeListener);
                    } else {
                        for (NodeRequest request : selectNodeRequests(targetShards)) {
                            sendOneNodeRequest(targetShards, computeListener, request);
                        }
                    }
                } finally {
                    sendingLock.unlock();
                }
            }
        } finally {
            listener.onResponse(null);
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
        final ActionListener<List<DriverProfile>> listener = computeListener.acquireCompute();
        sendRequest(request.node, request.shardIds, request.aliasFilters, new NodeListener() {
            void onAfter(List<DriverProfile> profiles) {
                nodePermits.get(request.node).release();
                if (concurrentRequests != null) {
                    concurrentRequests.release();
                }
                trySendingRequestsForPendingShards(targetShards, computeListener);
                listener.onResponse(profiles);
            }

            @Override
            public void onResponse(DataNodeComputeResponse response) {
                // remove failures of successful shards
                for (ShardId shardId : request.shardIds()) {
                    if (response.shardLevelFailures().containsKey(shardId) == false) {
                        shardFailures.remove(shardId);
                    }
                }
                for (Map.Entry<ShardId, Exception> e : response.shardLevelFailures().entrySet()) {
                    final ShardId shardId = e.getKey();
                    trackShardLevelFailure(shardId, false, e.getValue());
                    pendingShardIds.add(shardId);
                }
                onAfter(response.profiles());
            }

            @Override
            public void onFailure(Exception e, boolean receivedData) {
                for (ShardId shardId : request.shardIds) {
                    trackShardLevelFailure(shardId, receivedData, e);
                    pendingShardIds.add(shardId);
                }
                onAfter(List.of());
            }

            @Override
            public void onSkip() {
                skippedShards.incrementAndGet();
                if (rootTask.isCancelled()) {
                    onFailure(new TaskCancelledException("null"), true);
                } else {
                    onResponse(new DataNodeComputeResponse(List.of(), Map.of()));
                }
            }
        });
    }

    abstract void sendRequest(DiscoveryNode node, List<ShardId> shardIds, Map<Index, AliasFilter> aliasFilters, NodeListener nodeListener);

    interface NodeListener {
        void onResponse(DataNodeComputeResponse response);

        void onFailure(Exception e, boolean receivedData);

        void onSkip();
    }

    private static Exception unwrapFailure(Exception e) {
        e = e instanceof TransportException te ? FailureCollector.unwrapTransportException(te) : e;
        if (TransportActions.isShardNotAvailableException(e)) {
            return NoShardAvailableActionException.forOnShardFailureWrapper(e.getMessage());
        } else {
            return e;
        }
    }

    private void trackShardLevelFailure(ShardId shardId, boolean fatal, Exception originalEx) {
        final Exception e = unwrapFailure(originalEx);
        final boolean isTaskCanceledException = ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null;
        final boolean isCircuitBreakerException = ExceptionsHelper.unwrap(e, CircuitBreakingException.class) != null;
        shardFailures.compute(shardId, (k, current) -> {
            boolean mergedFatal = fatal || isTaskCanceledException || isCircuitBreakerException;
            return current == null
                ? new ShardFailure(mergedFatal, e)
                : new ShardFailure(
                    mergedFatal || current.fatal,
                    // Retain only one meaningful exception and avoid suppressing previous failures to minimize memory usage,
                    // especially when handling many shards.
                    isTaskCanceledException || e instanceof NoShardAvailableActionException ? current.failure : e
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
     * (Remaining) allocated nodes of a given shard id and its alias filter
     */
    record TargetShard(ShardId shardId, List<DiscoveryNode> remainingNodes, AliasFilter aliasFilter) {}

    record NodeRequest(DiscoveryNode node, List<ShardId> shardIds, Map<Index, AliasFilter> aliasFilters) {}

    private record ShardFailure(boolean fatal, Exception failure) {}

    private static boolean isRetryableFailure(ShardFailure failure) {
        return failure != null && failure.fatal == false && failure.failure instanceof NoShardAvailableActionException;
    }

    /**
     * Selects the next nodes to send requests to. Limits to at most one outstanding request per node.
     * If there is already a request in-flight to a node, another request will not be sent to the same node
     * until the first request completes. Instead, the next node in the remaining nodes will be tried.
     */
    private List<NodeRequest> selectNodeRequests(TargetShards targetShards) {
        assert sendingLock.isHeldByCurrentThread();
        final Map<DiscoveryNode, List<ShardId>> nodeToShardIds = new LinkedHashMap<>();
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
                List<ShardId> pendingRequest = nodeToShardIds.get(node);
                if (pendingRequest != null) {
                    pendingRequest.add(shard.shardId);
                    nodesIt.remove();
                    shardsIt.remove();
                    break;
                }

                if (concurrentRequests == null || concurrentRequests.tryAcquire()) {
                    if (nodePermits.computeIfAbsent(node, n -> new Semaphore(1)).tryAcquire()) {
                        pendingRequest = new ArrayList<>();
                        pendingRequest.add(shard.shardId);
                        nodeToShardIds.put(node, pendingRequest);

                        nodesIt.remove();
                        shardsIt.remove();

                        break;
                    } else if (concurrentRequests != null) {
                        concurrentRequests.release();
                    }
                }
            }
        }

        final List<NodeRequest> nodeRequests = new ArrayList<>(nodeToShardIds.size());
        for (var entry : nodeToShardIds.entrySet()) {
            var node = entry.getKey();
            var shardIds = entry.getValue();
            Map<Index, AliasFilter> aliasFilters = new HashMap<>();
            for (ShardId shardId : shardIds) {
                var aliasFilter = targetShards.getShard(shardId).aliasFilter;
                if (aliasFilter != null) {
                    aliasFilters.put(shardId.getIndex(), aliasFilter);
                }
            }
            nodeRequests.add(new NodeRequest(node, shardIds, aliasFilters));
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
            Map<String, DiscoveryNode> nodes = Maps.newHashMapWithExpectedSize(resp.getNodes().size());
            for (DiscoveryNode node : resp.getNodes()) {
                nodes.put(node.getId(), node);
            }
            int totalShards = 0;
            int skippedShards = 0;
            Map<ShardId, TargetShard> shards = Maps.newHashMapWithExpectedSize(resp.getGroups().size());
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
                shards.put(shardId, new TargetShard(shardId, allocatedNodes, aliasFilter));
            }
            return new TargetShards(shards, totalShards, skippedShards);
        });
        var searchShardsRequest = new SearchShardsRequest(
            originalIndices.indices(),
            originalIndices.indicesOptions(),
            requestFilter,
            null,
            null,
            true, // unavailable_shards will be handled by the sender
            clusterAlias
        );
        transportService.sendChildRequest(
            transportService.getLocalNode(),
            EsqlSearchShardsAction.TYPE.name(),
            searchShardsRequest,
            rootTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(searchShardsListener, SearchShardsResponse::new, esqlExecutor)
        );
    }

    /**
     * Attempts to resolve shards locations after they have been moved
     */
    Map<ShardId, List<DiscoveryNode>> resolveShards(Set<ShardId> shardIds) {
        var state = clusterService.state();
        var nodes = Maps.<ShardId, List<DiscoveryNode>>newMapWithExpectedSize(shardIds.size());
        for (var shardId : shardIds) {
            nodes.put(
                shardId,
                state.routingTable()
                    .shardRoutingTable(shardId)
                    .allShards()
                    .filter(shard -> shard.active() && shard.isSearchable())
                    .map(shard -> state.nodes().get(shard.currentNodeId()))
                    .toList()
            );
        }
        return nodes;
    }
}
