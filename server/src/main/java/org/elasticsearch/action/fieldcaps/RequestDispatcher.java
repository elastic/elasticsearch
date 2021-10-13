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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Dispatches child field-caps requests to old/new data nodes in the local cluster that have shards of the requesting indices.
 */
final class RequestDispatcher {
    public static final Logger LOGGER = LogManager.getLogger(RequestDispatcher.class);

    private final TransportService transportService;
    private final ClusterState clusterState;
    private final FieldCapabilitiesRequest fieldCapsRequest;
    private final Task parentTask;
    private final OriginalIndices originalIndices;
    private final long nowInMillis;

    private final boolean hasFilter;
    private final Consumer<FieldCapabilitiesIndexResponse> onIndexResponse;
    private final BiConsumer<String, Exception> onIndexFailure;

    private int pendingRequests = 0;
    private int executionRound = -1;
    private final Map<String, Map<String, List<ShardRouting>>> indices; // index -> node -> shards
    private final Map<String, Exception> failures = new HashMap<>();

    RequestDispatcher(ClusterService clusterService, TransportService transportService, Task parentTask,
                      FieldCapabilitiesRequest fieldCapsRequest, OriginalIndices originalIndices, long nowInMillis, String[] indices,
                      Consumer<FieldCapabilitiesIndexResponse> onIndexResponse, BiConsumer<String, Exception> onIndexFailure) {
        this.transportService = transportService;
        this.fieldCapsRequest = fieldCapsRequest;
        this.parentTask = parentTask;
        this.originalIndices = originalIndices;
        this.nowInMillis = nowInMillis;
        this.clusterState = clusterService.state();
        this.hasFilter =
            fieldCapsRequest.indexFilter() != null && fieldCapsRequest.indexFilter() instanceof MatchAllQueryBuilder == false;
        this.onIndexResponse = onIndexResponse;
        this.onIndexFailure = onIndexFailure;
        this.indices = new HashMap<>(indices.length);
        for (String index : indices) {
            final GroupShardsIterator<ShardIterator> shardIts =
                clusterService.operationRouting().searchShards(clusterState, new String[]{index}, null, null, null, null);
            final Map<String, List<ShardRouting>> nodeToShards = new HashMap<>();
            for (ShardIterator shardIt : shardIts) {
                for (ShardRouting shard : shardIt) {
                    nodeToShards.computeIfAbsent(shard.currentNodeId(), node -> new ArrayList<>()).add(shard);
                }
            }
            if (nodeToShards.isEmpty()) {
                onIndexFailure.accept(index, new ElasticsearchException("index [{}] has no active shard copy", index));
            } else {
                this.indices.put(index, nodeToShards);
            }
        }
    }

    void execute() {
        assert Thread.holdsLock(this) == false;
        final Map<String, List<ShardId>> nodeToSelectedShards = new HashMap<>();
        synchronized (this) {
            if (indices.isEmpty()) {
                return;
            }
            executionRound++;
            assert pendingRequests == 0 : "pending requests = " + pendingRequests;
            final List<String> failedIndices = new ArrayList<>();
            for (Map.Entry<String, Map<String, List<ShardRouting>>> e : indices.entrySet()) {
                final String index = e.getKey();
                // Do not try again if all shards in the previous round didn't match the filter.
                if (executionRound > 0 && failures.containsKey(index) == false) {
                    failedIndices.add(index);
                    assert hasFilter;
                    continue;
                }
                final List<ShardRouting> selectedShards = nextTarget(e.getValue(), clusterState.nodes(), hasFilter);
                if (selectedShards.isEmpty()) {
                    failedIndices.add(index);
                } else {
                    pendingRequests += selectedShards.size();
                    for (ShardRouting shard : selectedShards) {
                        nodeToSelectedShards.computeIfAbsent(shard.currentNodeId(), n -> new ArrayList<>()).add(shard.shardId());
                    }
                }
            }
            // report failed indices
            for (String failedIndex : failedIndices) {
                if (indices.remove(failedIndex) != null) {
                    final Exception failure = failures.remove(failedIndex);
                    if (failure != null) {
                        onIndexFailure.accept(failedIndex, failure);
                    } else {
                        onIndexResponse.accept(new FieldCapabilitiesIndexResponse(failedIndex, Collections.emptyMap(), false));
                    }
                }
            }
        }
        // send requests
        for (Map.Entry<String, List<ShardId>> e : nodeToSelectedShards.entrySet()) {
            sendRequestToNode(e.getKey(), e.getValue());
        }
    }

    static List<ShardRouting> nextTarget(Map<String, List<ShardRouting>> nodeToShards,
                                         DiscoveryNodes discoveryNodes,
                                         boolean withQueryFilter) {
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
            final Map.Entry<String, List<ShardRouting>> node = nodeIt.next();
            // If the target node is on the new version, then we can ask it to process all its copies in a single request
            // and the target node will process at most one valid copy. Otherwise, we should ask for a single copy to avoid
            // sending multiple requests.
            final DiscoveryNode discoNode = discoveryNodes.get(node.getKey());
            if (discoNode.getVersion().onOrAfter(Version.V_7_16_0)) {
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

    private void sendRequestToNode(String nodeId, List<ShardId> shardIds) {
        final DiscoveryNode node = clusterState.nodes().get(nodeId);
        assert node != null;
        if (node.getVersion().onOrAfter(Version.V_7_16_0)) {
            final ActionListener<FieldCapabilitiesNodeResponse> listener =
                ActionListener.wrap(r -> onNodeResponse(shardIds, r), failure -> onNodeFailure(shardIds, failure));
            final FieldCapabilitiesNodeRequest nodeRequest = new FieldCapabilitiesNodeRequest(
                shardIds,
                fieldCapsRequest.fields(),
                originalIndices,
                fieldCapsRequest.indexFilter(),
                nowInMillis,
                fieldCapsRequest.runtimeFields());
            LOGGER.debug("round {} sends field caps node request to node {} for shardIds {}", executionRound, node, shardIds);
            transportService.sendChildRequest(node, TransportFieldCapabilitiesAction.ACTION_NODE_NAME, nodeRequest, parentTask,
                TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, FieldCapabilitiesNodeResponse::new));
        } else {
            for (ShardId shardId : shardIds) {
                final ActionListener<FieldCapabilitiesIndexResponse> listener =
                    ActionListener.wrap(
                        r -> onNodeResponse(Collections.singletonList(shardId),
                            new FieldCapabilitiesNodeResponse(Collections.singletonList(r), Collections.emptyList())),
                        failure -> onNodeFailure(Collections.singletonList(shardId), failure));
                final FieldCapabilitiesIndexRequest shardRequest = new FieldCapabilitiesIndexRequest(
                    fieldCapsRequest.fields(), shardId, originalIndices, fieldCapsRequest.indexFilter(),
                    nowInMillis, fieldCapsRequest.runtimeFields());
                LOGGER.debug("round {} sends field caps shard request to node {} for shardId {}", executionRound, node, shardId);
                transportService.sendChildRequest(node, TransportFieldCapabilitiesAction.ACTION_SHARD_NAME, shardRequest, parentTask,
                    TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, FieldCapabilitiesIndexResponse::new));
            }
        }
    }

    private void onNodeResponse(List<ShardId> shardIds, FieldCapabilitiesNodeResponse nodeResponse) {
        assert Thread.holdsLock(this) == false;
        final boolean executeNext;
        final List<FieldCapabilitiesIndexResponse> matchedResponses = new ArrayList<>();
        synchronized (this) {
            for (FieldCapabilitiesIndexResponse indexResponse : nodeResponse.getIndexResponses()) {
                if (indexResponse.canMatch()) {
                    if (indices.remove(indexResponse.getIndexName()) != null) {
                        matchedResponses.add(indexResponse);
                    }
                    failures.remove(indexResponse.getIndexName());
                }
            }
            for (FieldCapabilitiesFailure failure : nodeResponse.getFailures()) {
                for (String index : failure.getIndices()) {
                    failures.put(index, failure.getException());
                }
            }
            // Here we only retry after all pending requests have responded to avoid exploding network requests
            // when the cluster is unstable or overloaded as an eager retry approach can add more load to the cluster.
            pendingRequests -= shardIds.size();
            executeNext = pendingRequests == 0;
        }
        for (FieldCapabilitiesIndexResponse indexResponse : matchedResponses) {
            onIndexResponse.accept(indexResponse);
        }
        if (executeNext) {
            execute();
        }
    }

    private void onNodeFailure(List<ShardId> shardIds, Exception e) {
        assert Thread.holdsLock(this) == false;
        final boolean executeNext;
        synchronized (this) {
            for (ShardId shardId : shardIds) {
                failures.put(shardId.getIndexName(), e);
            }
            pendingRequests -= shardIds.size();
            executeNext = pendingRequests == 0;
        }
        if (executeNext) {
            execute();
        }
    }
}
