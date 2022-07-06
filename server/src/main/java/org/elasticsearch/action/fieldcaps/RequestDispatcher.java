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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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

    static final Version GROUP_REQUESTS_VERSION = Version.V_7_16_0;

    static final Logger LOGGER = LogManager.getLogger(RequestDispatcher.class);

    private final TransportService transportService;
    private final ClusterState clusterState;
    private final FieldCapabilitiesRequest fieldCapsRequest;
    private final Task parentTask;
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
        this.hasFilter = fieldCapsRequest.indexFilter() != null && fieldCapsRequest.indexFilter() instanceof MatchAllQueryBuilder == false;
        this.executor = executor;
        this.onIndexResponse = onIndexResponse;
        this.onIndexFailure = onIndexFailure;
        this.onComplete = new RunOnce(onComplete);
        this.indexSelectors = ConcurrentCollections.newConcurrentMap();
        for (String index : indices) {
            final GroupShardsIterator<ShardIterator> shardIts = clusterService.operationRouting()
                .searchShards(clusterState, new String[] { index }, null, null, null, null);
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
            final List<ShardRouting> selectedShards = indexSelector.nextTarget(clusterState.nodes(), hasFilter);
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
        if (node.getVersion().onOrAfter(GROUP_REQUESTS_VERSION)) {
            LOGGER.debug("round {} sends field caps node request to node {} for shardIds {}", executionRound, node, shardIds);
            final ActionListener<FieldCapabilitiesNodeResponse> listener = ActionListener.wrap(
                r -> onRequestResponse(shardIds, r),
                failure -> onRequestFailure(shardIds, failure)
            );
            final FieldCapabilitiesNodeRequest nodeRequest = new FieldCapabilitiesNodeRequest(
                shardIds,
                fieldCapsRequest.fields(),
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
        } else {
            for (ShardId shardId : shardIds) {
                LOGGER.debug("round {} sends field caps shard request to node {} for shardId {}", executionRound, node, shardId);
                final ActionListener<FieldCapabilitiesIndexResponse> listener = ActionListener.wrap(r -> {
                    final FieldCapabilitiesNodeResponse nodeResponse;
                    if (r.canMatch()) {
                        nodeResponse = new FieldCapabilitiesNodeResponse(
                            Collections.singletonList(r),
                            Collections.emptyMap(),
                            Collections.emptySet()
                        );
                    } else {
                        nodeResponse = new FieldCapabilitiesNodeResponse(
                            Collections.emptyList(),
                            Collections.emptyMap(),
                            Collections.singleton(shardId)
                        );
                    }
                    onRequestResponse(Collections.singletonList(shardId), nodeResponse);
                }, e -> onRequestFailure(Collections.singletonList(shardId), e));
                final FieldCapabilitiesIndexRequest shardRequest = new FieldCapabilitiesIndexRequest(
                    fieldCapsRequest.fields(),
                    shardId,
                    originalIndices,
                    fieldCapsRequest.indexFilter(),
                    nowInMillis,
                    fieldCapsRequest.runtimeFields()
                );
                transportService.sendChildRequest(
                    node,
                    TransportFieldCapabilitiesAction.ACTION_SHARD_NAME,
                    shardRequest,
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(
                        listener,
                        is -> new FieldCapabilitiesIndexResponse(is, new IndexFieldCapabilities.Deduplicator())
                    )
                );
            }
        }
    }

    private void afterRequestsCompleted(int numRequests) {
        if (pendingRequests.addAndGet(-numRequests) == 0) {
            // Here we only retry after all pending requests have responded to avoid exploding network requests
            // when the cluster is unstable or overloaded as an eager retry approach can add more load to the cluster.
            executionRound.incrementAndGet();
            execute();
        }
    }

    private void onRequestResponse(List<ShardId> shardIds, FieldCapabilitiesNodeResponse nodeResponse) {
        for (FieldCapabilitiesIndexResponse indexResponse : nodeResponse.getIndexResponses()) {
            if (indexResponse.canMatch()) {
                if (indexSelectors.remove(indexResponse.getIndexName()) != null) {
                    onIndexResponse.accept(indexResponse);
                }
            }
        }
        for (ShardId unmatchedShardId : nodeResponse.getUnmatchedShardIds()) {
            final IndexSelector indexSelector = indexSelectors.get(unmatchedShardId.getIndexName());
            if (indexSelector != null) {
                indexSelector.addUnmatchedShardId(unmatchedShardId);
            }
        }
        for (Map.Entry<ShardId, Exception> e : nodeResponse.getFailures().entrySet()) {
            final IndexSelector indexSelector = indexSelectors.get(e.getKey().getIndexName());
            if (indexSelector != null) {
                indexSelector.setFailure(e.getKey(), e.getValue());
            }
        }
        afterRequestsCompleted(shardIds.size());
    }

    private void onRequestFailure(List<ShardId> shardIds, Exception e) {
        for (ShardId shardId : shardIds) {
            final IndexSelector indexSelector = indexSelectors.get(shardId.getIndexName());
            if (indexSelector != null) {
                indexSelector.setFailure(shardId, e);
            }
        }
        afterRequestsCompleted(shardIds.size());
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
                first = ExceptionsHelper.useOrSuppress(first, e);
            }
            return first;
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

        synchronized List<ShardRouting> nextTarget(DiscoveryNodes discoveryNodes, boolean withQueryFilter) {
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
                // If the target node is on the new version, then we can ask it to process all its copies in a single request
                // and the target node will process at most one valid copy. Otherwise, we should ask for a single copy to avoid
                // sending multiple requests.
                final DiscoveryNode discoNode = discoveryNodes.get(node.getKey());
                if (discoNode.getVersion().onOrAfter(GROUP_REQUESTS_VERSION)) {
                    nodeIt.remove();
                    return node.getValue();
                } else {
                    final List<ShardRouting> shards = node.getValue();
                    final ShardRouting selectedShard = shards.remove(0);
                    if (shards.isEmpty()) {
                        nodeIt.remove();
                    }
                    return Collections.singletonList(selectedShard);
                }
            }
        }
    }
}
