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
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handles computes within a single cluster by dispatching {@link DataNodeRequest} to data nodes
 * and executing these computes on the data nodes.
 */
abstract class DataNodeRequestSender {
    private final TransportService transportService;
    private final Executor esqlExecutor;
    private final CancellableTask rootTask;
    private final ReentrantLock sendingLock = new ReentrantLock();
    private final Queue<ShardId> pendingShardIds = ConcurrentCollections.newQueue();
    private final Map<DiscoveryNode, Semaphore> nodePermits = new HashMap<>();
    private final Map<ShardId, Exception> shardFailures = ConcurrentCollections.newConcurrentMap();
    private final AtomicBoolean changed = new AtomicBoolean();
    private final AtomicBoolean aborted = new AtomicBoolean();

    DataNodeRequestSender(TransportService transportService, Executor esqlExecutor, CancellableTask rootTask) {
        this.transportService = transportService;
        this.esqlExecutor = esqlExecutor;
        this.rootTask = rootTask;
    }

    final void startComputeOnDataNodes(
        String clusterAlias,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        QueryBuilder requestFilter,
        Runnable runOnTaskFailure,
        ActionListener<ComputeResponse> listener
    ) {
        final long startTimeInNanos = System.nanoTime();
        searchShards(rootTask, clusterAlias, requestFilter, concreteIndices, originalIndices, ActionListener.wrap(targetShards -> {
            try (var computeListener = new ComputeListener(transportService.getThreadPool(), runOnTaskFailure, listener.map(profiles -> {
                TimeValue took = TimeValue.timeValueNanos(System.nanoTime() - startTimeInNanos);
                return new ComputeResponse(
                    profiles,
                    took,
                    targetShards.totalShards(),
                    targetShards.totalShards(),
                    targetShards.skippedShards(),
                    0
                );
            }))) {
                for (TargetShard shard : targetShards.shards.values()) {
                    for (DiscoveryNode node : shard.remainingNodes) {
                        nodePermits.putIfAbsent(node, new Semaphore(1));
                    }
                }
                pendingShardIds.addAll(targetShards.shards.keySet());
                trySendingRequestsForPendingShards(targetShards, computeListener);
            }
        }, listener::onFailure));
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
                    for (ShardId shardId : pendingShardIds) {
                        if (targetShards.getShard(shardId).remainingNodes.isEmpty()) {
                            final Exception failure = shardFailures.get(shardId);
                            if (failure == null) {
                                shardFailures.put(shardId, new ShardNotFoundException(shardId, "no shard copies found {}", shardId));
                            }
                            aborted.set(true);
                        }
                    }
                    if (aborted.get()) {
                        for (Exception e : shardFailures.values()) {
                            computeListener.acquireAvoid().onFailure(e);
                        }
                    } else {
                        var nodeRequests = selectNodeRequests(targetShards);
                        for (NodeRequest request : nodeRequests) {
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

    private void sendOneNodeRequest(TargetShards targetShards, ComputeListener computeListener, NodeRequest request) {
        final ActionListener<List<DriverProfile>> listener = computeListener.acquireCompute();
        sendRequest(request.node, request.shardIds, request.aliasFilters, new NodeListener() {
            void onAfter(List<DriverProfile> profiles) {
                nodePermits.get(request.node).release();
                trySendingRequestsForPendingShards(targetShards, computeListener);
                listener.onResponse(profiles);
            }

            @Override
            public void onResponse(DataNodeComputeResponse response) {
                // remove failures of successful shards
                for (ShardId shardId : targetShards.shardIds()) {
                    if (shardFailures.containsKey(shardId) == false) {
                        shardFailures.remove(shardId);
                    }
                }
                for (Map.Entry<ShardId, Exception> e : response.shardLevelFailures().entrySet()) {
                    final ShardId shardId = e.getKey();
                    shardFailures.compute(shardId, (k, v) -> mergeShardExceptions(v, e.getValue()));
                    pendingShardIds.add(shardId);
                }
                onAfter(response.profiles());
            }

            @Override
            public void onFailure(Exception e, boolean receivedData) {
                if (receivedData) {
                    aborted.set(true);
                }
                for (ShardId shardId : request.shardIds) {
                    shardFailures.compute(shardId, (k, v) -> mergeShardExceptions(v, e));
                    pendingShardIds.add(shardId);
                }
                onAfter(List.of());
            }
        });
    }

    abstract void sendRequest(DiscoveryNode node, List<ShardId> shardIds, Map<Index, AliasFilter> aliasFilters, NodeListener nodeListener);

    interface NodeListener {
        void onResponse(DataNodeComputeResponse response);

        void onFailure(Exception e, boolean receivedData);
    }

    private static Exception mergeShardExceptions(Exception current, Exception e) {
        e = e instanceof TransportException te ? FailureCollector.unwrapTransportException(te) : e;
        if (TransportActions.isShardNotAvailableException(e)) {
            e = NoShardAvailableActionException.forOnShardFailureWrapper(e.getMessage());
        }
        if (current == null) {
            return e;
        }
        if (current instanceof NoShardAvailableActionException || ExceptionsHelper.unwrap(current, TaskCancelledException.class) != null) {
            return e;
        }
        if (e instanceof NoShardAvailableActionException || ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null) {
            return current;
        }
        if (current != e) {
            current.addSuppressed(e);
        }
        return current;
    }

    /**
     * Result from {@link #searchShards(Task, String, QueryBuilder, Set, OriginalIndices, ActionListener)} where can_match is performed to
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

        Set<ShardId> shardIds() {
            return shards.keySet();
        }
    }

    /**
     * (Remaining) allocated nodes of a given shard id and its alias filter
     */
    record TargetShard(ShardId shardId, List<DiscoveryNode> remainingNodes, AliasFilter aliasFilter) {

    }

    record NodeRequest(DiscoveryNode node, List<ShardId> shardIds, Map<Index, AliasFilter> aliasFilters) {

    }

    /**
     * Selects the next nodes to send requests to. Limits to at most one outstanding request per node.
     * If there is already a request in-flight to a node, another request will not be sent to the same node
     * until the first request completes. Instead, the next node in the remaining nodes will be tried.
     */
    private List<NodeRequest> selectNodeRequests(TargetShards targetShards) {
        assert sendingLock.isHeldByCurrentThread();
        final Map<DiscoveryNode, List<ShardId>> nodeToShardIds = new HashMap<>();
        final Iterator<ShardId> shardsIt = pendingShardIds.iterator();
        while (shardsIt.hasNext()) {
            ShardId shardId = shardsIt.next();
            TargetShard shard = targetShards.getShard(shardId);
            Iterator<DiscoveryNode> nodesIt = shard.remainingNodes.iterator();
            DiscoveryNode selectedNode = null;
            while (nodesIt.hasNext()) {
                DiscoveryNode node = nodesIt.next();
                if (nodeToShardIds.containsKey(node) || nodePermits.get(node).tryAcquire()) {
                    nodesIt.remove();
                    shardsIt.remove();
                    selectedNode = node;
                    break;
                }
            }
            if (selectedNode != null) {
                nodeToShardIds.computeIfAbsent(selectedNode, unused -> new ArrayList<>()).add(shard.shardId);
            }
        }
        final List<NodeRequest> nodeRequests = new ArrayList<>(nodeToShardIds.size());
        for (var e : nodeToShardIds.entrySet()) {
            List<ShardId> shardIds = e.getValue();
            Map<Index, AliasFilter> aliasFilters = new HashMap<>();
            for (ShardId shardId : shardIds) {
                var aliasFilter = targetShards.getShard(shardId).aliasFilter;
                if (aliasFilter != null) {
                    aliasFilters.put(shardId.getIndex(), aliasFilter);
                }
            }
            nodeRequests.add(new NodeRequest(e.getKey(), shardIds, aliasFilters));
        }
        return nodeRequests;
    }

    /**
     * Performs can_match and find the target nodes for the given target indices and filter.
     * <p>
     * Ideally, the search_shards API should be called before the field-caps API; however, this can lead
     * to a situation where the column structure (i.e., matched data types) differs depending on the query.
     */
    void searchShards(
        Task parentTask,
        String clusterAlias,
        QueryBuilder filter,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        ActionListener<TargetShards> listener
    ) {
        ActionListener<SearchShardsResponse> searchShardsListener = listener.map(resp -> {
            Map<String, DiscoveryNode> nodes = new HashMap<>();
            for (DiscoveryNode node : resp.getNodes()) {
                nodes.put(node.getId(), node);
            }
            int totalShards = 0;
            int skippedShards = 0;
            Map<ShardId, TargetShard> shards = new HashMap<>();
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
        SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
            originalIndices.indices(),
            originalIndices.indicesOptions(),
            filter,
            null,
            null,
            false,
            clusterAlias
        );
        transportService.sendChildRequest(
            transportService.getLocalNode(),
            EsqlSearchShardsAction.TYPE.name(),
            searchShardsRequest,
            parentTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(searchShardsListener, SearchShardsResponse::new, esqlExecutor)
        );
    }
}
