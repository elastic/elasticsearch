/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
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
    private final CancellableTask parentTask;
    private final OriginalIndices originalIndices;
    private final long nowInMillis;

    private final boolean hasFilter;
    private final Executor executor;
    private final Consumer<FieldCapabilitiesIndexResponse> onIndexResponse;
    private final BiConsumer<String, Exception> onIndexFailure;
    private final Runnable onComplete;

    private final AtomicInteger pendingRequests = new AtomicInteger();
    private final AtomicInteger executionRound = new AtomicInteger();
    private final Map<String, IndexSelector> indexSelectors;
    private final IndicesService indicesService;

    RequestDispatcher(
        IndicesService indicesService,
        TransportService transportService,
        CancellableTask parentTask,
        FieldCapabilitiesRequest fieldCapsRequest,
        OriginalIndices originalIndices,
        long nowInMillis,
        String[] indices,
        Executor executor,
        Consumer<FieldCapabilitiesIndexResponse> onIndexResponse,
        BiConsumer<String, Exception> onIndexFailure,
        Runnable onComplete
    ) {
        this.indicesService = indicesService;
        this.transportService = transportService;
        this.fieldCapsRequest = fieldCapsRequest;
        this.parentTask = parentTask;
        this.originalIndices = originalIndices;
        this.nowInMillis = nowInMillis;
        var clusterService = indicesService.clusterService();
        this.clusterState = clusterService.state();
        this.hasFilter = fieldCapsRequest.indexFilter() != null && fieldCapsRequest.indexFilter() instanceof MatchAllQueryBuilder == false;
        this.executor = executor;
        this.onIndexResponse = onIndexResponse;
        this.onIndexFailure = onIndexFailure;
        this.onComplete = new RunOnce(onComplete);
        this.indexSelectors = ConcurrentCollections.newConcurrentMap();
        for (String index : indices) {
            final GroupShardsIterator<ShardIterator> shardIts;
            try {
                shardIts = clusterService.operationRouting().searchShards(clusterState, new String[] { index }, null, null);
            } catch (Exception e) {
                onIndexFailure.accept(index, e);
                continue;
            }
            final IndexSelector indexResult = new IndexSelector(shardIts);
            if (indexResult.nodeToShards.isEmpty()) {
                onIndexFailure.accept(index, new NoShardAvailableActionException(null, "index [" + index + "] has no active shard copy"));
            } else {
                this.indexSelectors.put(index, indexResult);
            }
        }
    }

    void execute() {
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                // If we get rejected, mark pending indices as failed and complete
                final List<String> failedIndices = new ArrayList<>(indexSelectors.keySet());
                for (String failedIndex : failedIndices) {
                    final IndexSelector removed = indexSelectors.remove(failedIndex);
                    assert removed != null;
                    onIndexFailure.accept(failedIndex, e);
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
        final List<String> failedIndices = new ArrayList<>();
        for (Map.Entry<String, IndexSelector> e : indexSelectors.entrySet()) {
            final String index = e.getKey();
            final IndexSelector indexSelector = e.getValue();
            final List<ShardRouting> selectedShards = indexSelector.nextTarget(hasFilter);
            if (selectedShards.isEmpty()) {
                failedIndices.add(index);
            } else {
                pendingRequests.addAndGet(selectedShards.size());
                for (ShardRouting shard : selectedShards) {
                    nodeToSelectedShards.computeIfAbsent(shard.currentNodeId(), n -> new ArrayList<>()).add(shard.shardId());
                }
            }
        }
        for (String failedIndex : failedIndices) {
            final IndexSelector indexSelector = indexSelectors.remove(failedIndex);
            assert indexSelector != null;
            final Exception failure = indexSelector.getFailure();
            if (failure != null) {
                onIndexFailure.accept(failedIndex, failure);
            }
        }
        if (nodeToSelectedShards.isEmpty()) {
            onComplete.run();
        } else {
            var localShardIds = nodeToSelectedShards.remove(clusterState.nodes().getLocalNodeId());
            for (Map.Entry<String, List<ShardId>> e : nodeToSelectedShards.entrySet()) {
                sendRequestToNode(e.getKey(), makeNodeRequest(e.getValue()));
            }
            if (localShardIds != null) {
                try {
                    TransportFieldCapabilitiesAction.runOnLocalNode(
                        indicesService,
                        makeNodeRequest(localShardIds),
                        parentTask,
                        onIndexResponse,
                        this::consumeUnmatchedShardId,
                        this::consumeFailure
                    );
                } catch (Exception e) {
                    onRequestFailure(localShardIds, e);
                } finally {
                    afterRequestsCompleted(localShardIds.size());
                }
            }
        }
    }

    private FieldCapabilitiesNodeRequest makeNodeRequest(List<ShardId> shardIds) {
        return new FieldCapabilitiesNodeRequest(
            shardIds,
            fieldCapsRequest.fields(),
            fieldCapsRequest.filters(),
            fieldCapsRequest.types(),
            originalIndices,
            fieldCapsRequest.indexFilter(),
            nowInMillis,
            fieldCapsRequest.runtimeFields(),
            fieldCapsRequest.includeEmptyFields()
        );
    }

    // for testing
    int executionRound() {
        return executionRound.get();
    }

    private void sendRequestToNode(String nodeId, FieldCapabilitiesNodeRequest nodeRequest) {
        final DiscoveryNode node = clusterState.nodes().get(nodeId);
        assert node != null;
        LOGGER.debug("round {} sends field caps node request to node {} for shardIds {}", executionRound, node, nodeRequest.shardIds());
        transportService.sendChildRequest(
            node,
            TransportFieldCapabilitiesAction.ACTION_NODE_NAME,
            nodeRequest,
            parentTask,
            TransportRequestOptions.EMPTY,
            new TransportResponseHandler<>() {

                private Exception failure;

                @Override
                public Executor executor() {
                    return executor;
                }

                @Override
                public void handleResponse(TransportResponse response) {
                    var f = this.failure;
                    if (f != null) {
                        onFailure(f);
                        return;
                    }
                    afterRequestsCompleted(nodeRequest.shardIds().size());
                }

                @Override
                public void handleException(TransportException e) {
                    onFailure(e);
                }

                private void onFailure(Exception e) {
                    try {
                        onRequestFailure(nodeRequest.shardIds(), e);
                    } finally {
                        afterRequestsCompleted(nodeRequest.shardIds().size());
                    }
                }

                @Override
                public TransportResponse read(StreamInput in) throws IOException {
                    try {
                        FieldCapabilitiesIndexResponse.readList(in, RequestDispatcher.this::consumeIndexResponse);
                        int failures = in.readVInt();
                        for (int i = 0; i < failures; i++) {
                            consumeFailure(new ShardId(in), in.readException());
                        }
                        int unmatched = in.readVInt();
                        for (int i = 0; i < unmatched; i++) {
                            consumeUnmatchedShardId(new ShardId(in));
                        }
                    } catch (Exception e) {
                        in.skip(in.available());
                        failure = e;
                    }
                    return TransportResponse.Empty.INSTANCE;
                }
            }
        );
    }

    private void consumeFailure(ShardId shardId, Exception e) {
        final IndexSelector indexSelector = indexSelectors.get(shardId.getIndexName());
        if (indexSelector != null) {
            indexSelector.setFailure(shardId, e);
        }
    }

    private void consumeUnmatchedShardId(ShardId unmatchedShardId) {
        final IndexSelector indexSelector = indexSelectors.get(unmatchedShardId.getIndexName());
        if (indexSelector != null) {
            indexSelector.addUnmatchedShardId(unmatchedShardId);
        }
    }

    private void consumeIndexResponse(FieldCapabilitiesIndexResponse indexResponse) {
        if (indexResponse.canMatch()
            && (fieldCapsRequest.includeEmptyFields() == false || indexSelectors.remove(indexResponse.getIndexName()) != null)) {
            // we accept all the responses because they may vary from node to node if we exclude empty fields
            onIndexResponse.accept(indexResponse);
        }
    }

    private void afterRequestsCompleted(int numRequests) {
        int res = pendingRequests.addAndGet(-numRequests);
        assert res >= 0;
        if (res == 0) {
            // Here we only retry after all pending requests have responded to avoid exploding network requests
            // when the cluster is unstable or overloaded as an eager retry approach can add more load to the cluster.
            executionRound.incrementAndGet();
            execute();
        }
    }

    private void onRequestFailure(List<ShardId> shardIds, Exception e) {
        for (ShardId shardId : shardIds) {
            final IndexSelector indexSelector = indexSelectors.get(shardId.getIndexName());
            if (indexSelector != null) {
                indexSelector.setFailure(shardId, e);
            }
        }
    }

    private static class IndexSelector {
        private final Map<String, List<ShardRouting>> nodeToShards = new HashMap<>();
        private final Set<ShardId> unmatchedShardIds = new HashSet<>();
        private final Map<ShardId, Exception> failures = new HashMap<>();

        IndexSelector(GroupShardsIterator<ShardIterator> shardIts) {
            for (ShardIterator shardIt : shardIts) {
                for (ShardRouting shard : shardIt) {
                    nodeToShards.computeIfAbsent(shard.currentNodeId(), node -> new ArrayList<>()).add(shard);
                }
            }
        }

        synchronized Exception getFailure() {
            Exception first = null;
            for (Exception e : failures.values()) {
                first = useOrSuppressIfDifferent(first, e);
            }
            return first;
        }

        static Exception useOrSuppressIfDifferent(Exception first, Exception second) {
            if (first == null) {
                return second;
            } else if (ExceptionsHelper.unwrap(first) != ExceptionsHelper.unwrap(second)) {
                first.addSuppressed(second);
            }
            return first;
        }

        synchronized void setFailure(ShardId shardId, Exception failure) {
            assert unmatchedShardIds.contains(shardId) == false : "Shard " + shardId + " was unmatched already";
            failures.compute(shardId, (k, curr) -> useOrSuppressIfDifferent(curr, failure));
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
