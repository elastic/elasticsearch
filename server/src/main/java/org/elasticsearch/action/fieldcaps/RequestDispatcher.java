/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Dispatches child field-caps requests to old/new data nodes in the local cluster that have shards of the requesting indices.
 */
final class RequestDispatcher {
    static final Logger LOGGER = LogManager.getLogger(RequestDispatcher.class);

    private final TransportService transportService;
    private final ClusterState clusterState;
    private final FieldCapabilitiesRequest fieldCapsRequest;
    private final Task parentTask;
    private final OriginalIndices originalIndices;
    private final long nowInMillis;

    private final boolean withIndexFilter;
    private final Executor executor;
    private final Consumer<FieldCapabilitiesIndexResponse> onIndexResponse;
    private final BiConsumer<String, Exception> onIndexFailure;
    private final Runnable onComplete;

    private final AtomicInteger pendingRequests = new AtomicInteger();
    private final AtomicInteger executionRound = new AtomicInteger();
    private final Map<String, Group> groups = ConcurrentCollections.newConcurrentMap();

    RequestDispatcher(
        ClusterService clusterService,
        TransportService transportService,
        Task parentTask,
        FieldCapabilitiesRequest fieldCapsRequest,
        OriginalIndices originalIndices,
        long nowInMillis,
        String[] indices,
        Executor executor,
        Consumer<FieldCapabilitiesIndexResponse> onIndexResponse,
        BiConsumer<String, Exception> onIndexFailure,
        Runnable onComplete
    ) {
        this.transportService = transportService;
        this.fieldCapsRequest = fieldCapsRequest;
        this.parentTask = parentTask;
        this.originalIndices = originalIndices;
        this.nowInMillis = nowInMillis;
        this.clusterState = clusterService.state();
        this.withIndexFilter = fieldCapsRequest.indexFilter() != null
            && fieldCapsRequest.indexFilter() instanceof MatchAllQueryBuilder == false;
        this.executor = executor;
        this.onIndexResponse = onIndexResponse;
        this.onIndexFailure = onIndexFailure;
        this.onComplete = new RunOnce(onComplete);
        final List<Group> groupedIndices = groupIndicesByMappingHash(clusterService, clusterState, withIndexFilter, indices);
        for (Group group : groupedIndices) {
            if (group.nodeToShards.isEmpty()) {
                for (String index : group.indices) {
                    onIndexFailure.accept(
                        index,
                        new NoShardAvailableActionException(null, "index [" + index + "] has no active shard copy")
                    );
                }
            } else {
                for (String index : group.indices) {
                    this.groups.put(index, group);
                }
            }
        }
    }

    void execute() {
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                // If we get rejected, mark pending indices as failed and complete
                for (Group g : groups.values()) {
                    if (g.completed.compareAndSet(false, true)) {
                        g.indices.forEach(index -> onIndexFailure.accept(index, e));
                    }
                }
                onComplete.run();
            }

            @Override
            protected void doRun() {
                innerExecute();
            }
        });
    }

    private void innerExecute() {
        final Map<String, List<ShardId>> nodeToSelectedShards = new HashMap<>();
        assert pendingRequests.get() == 0 : "pending requests = " + pendingRequests;
        groups.values().removeIf(g -> g.completed.get());
        final Set<Group> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Group group : groups.values()) {
            if (visited.add(group)) {
                final List<ShardRouting> selectedShards = group.nextTarget(withIndexFilter);
                if (selectedShards.isEmpty()) {
                    if (group.completed.compareAndSet(false, true)) {
                        group.getFailures().forEach(onIndexFailure);
                    }
                } else {
                    for (ShardRouting shard : selectedShards) {
                        nodeToSelectedShards.computeIfAbsent(shard.currentNodeId(), n -> new ArrayList<>()).add(shard.shardId());
                    }
                }
            }
        }
        if (nodeToSelectedShards.isEmpty()) {
            assert groups.values().stream().allMatch(g -> g.completed.get()) : "Some groups aren't completed yet";
            onComplete.run();
        } else {
            pendingRequests.addAndGet(nodeToSelectedShards.size());
            for (Map.Entry<String, List<ShardId>> e : nodeToSelectedShards.entrySet()) {
                sendRequestToNode(e.getKey(), e.getValue());
            }
        }
    }

    // for testing
    int executionRound() {
        return executionRound.get();
    }

    private void sendRequestToNode(String nodeId, List<ShardId> shardIds) {
        final DiscoveryNode node = clusterState.nodes().get(nodeId);
        assert node != null;
        LOGGER.debug("round {} sends field caps node request to node {} for shardIds {}", executionRound, node, shardIds);
        final ActionListener<FieldCapabilitiesNodeResponse> listener = ActionListener.wrap(
            this::onRequestResponse,
            failure -> onRequestFailure(shardIds, failure)
        );
        final FieldCapabilitiesNodeRequest nodeRequest = new FieldCapabilitiesNodeRequest(
            shardIds,
            fieldCapsRequest.fields(),
            fieldCapsRequest.filters(),
            fieldCapsRequest.allowedTypes(),
            originalIndices,
            fieldCapsRequest.indexFilter(),
            nowInMillis,
            fieldCapsRequest.runtimeFields()
        );
        transportService.sendChildRequest(
            node,
            TransportFieldCapabilitiesAction.ACTION_NODE_NAME,
            nodeRequest,
            parentTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, FieldCapabilitiesNodeResponse::new)
        );
    }

    private void afterRequestsCompleted() {
        if (pendingRequests.decrementAndGet() == 0) {
            // Here we only retry after all pending requests have responded to avoid exploding network requests
            // when the cluster is unstable or overloaded as an eager retry approach can add more load to the cluster.
            executionRound.incrementAndGet();
            execute();
        }
    }

    private void onRequestResponse(FieldCapabilitiesNodeResponse nodeResponse) {
        for (FieldCapabilitiesIndexResponse indexResponse : nodeResponse.getIndexResponses()) {
            if (indexResponse.canMatch()) {
                final Group group = groups.remove(indexResponse.getIndexName());
                if (group == null) {
                    continue;
                }
                if (group.completed.compareAndSet(false, true)) {
                    final String mappingHash = group.mappingHash != null ? group.mappingHash : indexResponse.getIndexMappingHash();
                    for (String index : group.indices) {
                        onIndexResponse.accept(new FieldCapabilitiesIndexResponse(index, mappingHash, indexResponse.get(), true));
                    }
                }
            }
        }
        for (ShardId unmatchedShardId : nodeResponse.getUnmatchedShardIds()) {
            final Group group = groups.get(unmatchedShardId.getIndexName());
            if (group != null) {
                group.addUnmatchedShardId(unmatchedShardId);
            }
        }
        for (Map.Entry<ShardId, Exception> e : nodeResponse.getFailures().entrySet()) {
            final Group group = groups.get(e.getKey().getIndexName());
            if (group != null) {
                group.setFailure(e.getKey(), e.getValue());
            }
        }
        afterRequestsCompleted();
    }

    private void onRequestFailure(List<ShardId> shardIds, Exception e) {
        for (ShardId shardId : shardIds) {
            final Group group = groups.get(shardId.getIndexName());
            if (group != null) {
                group.setFailure(shardId, e);
            }
        }
        afterRequestsCompleted();
    }

    private List<Group> groupIndicesByMappingHash(
        ClusterService clusterService,
        ClusterState clusterState,
        boolean withIndexFilter,
        String[] indices
    ) {
        final Map<String, List<String>> withMappingHashes = new HashMap<>();
        final List<Group> groups = new ArrayList<>();
        for (String index : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().index(index);
            if (withIndexFilter == false
                && indexMetadata != null
                && indexMetadata.mapping() != null
                && indexMetadata.mapping().getSha256() != null) {
                withMappingHashes.computeIfAbsent(indexMetadata.mapping().getSha256(), k -> new ArrayList<>()).add(index);
            } else {
                final GroupShardsIterator<ShardIterator> shardIts = clusterService.operationRouting()
                    .searchShards(clusterState, new String[] { index }, null, null, null, null);
                groups.add(new Group(List.of(index), null, shardIts));
            }
        }
        for (Map.Entry<String, List<String>> e : withMappingHashes.entrySet()) {
            final GroupShardsIterator<ShardIterator> shardIts = clusterService.operationRouting()
                .searchShards(clusterState, e.getValue().toArray(String[]::new), null, null, null, null);
            groups.add(new Group(e.getValue(), e.getKey(), shardIts));
        }
        return groups;
    }

    /**
     * A group of indices that have the same mapping hash
     */
    private static class Group {
        private final List<String> indices;
        private final String mappingHash;
        private final Map<String, List<ShardRouting>> nodeToShards = new HashMap<>();
        private final Set<ShardId> unmatchedShardIds = new HashSet<>();
        private final Map<ShardId, Exception> failures = new HashMap<>();
        private final AtomicBoolean completed = new AtomicBoolean();

        Group(List<String> indices, String mappingHash, GroupShardsIterator<ShardIterator> shardIts) {
            this.indices = indices;
            this.mappingHash = mappingHash;
            for (ShardIterator shardIt : shardIts) {
                for (ShardRouting shard : shardIt) {
                    nodeToShards.computeIfAbsent(shard.currentNodeId(), node -> new ArrayList<>()).add(shard);
                }
            }
        }

        synchronized Map<String, Exception> getFailures() {
            final Map<String, Exception> perIndex = new HashMap<>();
            for (Map.Entry<ShardId, Exception> e : failures.entrySet()) {
                perIndex.compute(e.getKey().getIndexName(), (unused, curr) -> ExceptionsHelper.useOrSuppress(curr, e.getValue()));
            }
            return perIndex;
        }

        synchronized void setFailure(ShardId shardId, Exception failure) {
            assert unmatchedShardIds.contains(shardId) == false : "Shard " + shardId + " was unmatched already";
            failures.compute(shardId, (k, curr) -> ExceptionsHelper.useOrSuppress(curr, failure));
        }

        synchronized void addUnmatchedShardId(ShardId shardId) {
            final boolean added = unmatchedShardIds.add(shardId);
            assert added : "Shard " + shardId + " was unmatched already";
            failures.remove(shardId);
        }

        synchronized List<ShardRouting> nextTarget(boolean withQueryFilter) {
            if (nodeToShards.isEmpty()) {
                return Collections.emptyList();
            }
            final Iterator<Map.Entry<String, List<ShardRouting>>> nodeIt = nodeToShards.entrySet().iterator();
            if (withQueryFilter) {
                // If an index filter is specified, then we must reach out to all of an index's shards to check
                // if one of them can match. Otherwise, for efficiency we just reach out to one of its shards.
                final List<ShardRouting> selectedShards = new ArrayList<>();
                final Set<ShardId> selectedShardIds = new HashSet<>();
                while (nodeIt.hasNext()) {
                    final List<ShardRouting> shards = nodeIt.next().getValue();
                    final Iterator<ShardRouting> shardIt = shards.iterator();
                    while (shardIt.hasNext()) {
                        final ShardRouting shard = shardIt.next();
                        if (unmatchedShardIds.contains(shard.shardId())) {
                            shardIt.remove();
                            continue;
                        }
                        if (selectedShardIds.add(shard.shardId())) {
                            shardIt.remove();
                            selectedShards.add(shard);
                        }
                    }
                    if (shards.isEmpty()) {
                        nodeIt.remove();
                    }
                }
                return selectedShards;
            } else {
                assert unmatchedShardIds.isEmpty();
                final Map.Entry<String, List<ShardRouting>> node = nodeIt.next();
                nodeIt.remove();
                return node.getValue();
            }
        }
    }
}
