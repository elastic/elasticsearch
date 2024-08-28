/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Response;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction;

import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class TransportGetCheckpointAction extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetCheckpointAction.class);
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final TransportService transportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Client client;

    @Inject
    public TransportGetCheckpointAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final IndicesService indicesService,
        final ClusterService clusterService,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Client client
    ) {
        super(GetCheckpointAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState clusterState = clusterService.state();
        resolveIndicesAndGetCheckpoint(task, request, listener, clusterState);
    }

    protected void resolveIndicesAndGetCheckpoint(
        Task task,
        Request request,
        ActionListener<Response> listener,
        final ClusterState clusterState
    ) {
        final String nodeId = clusterState.nodes().getLocalNode().getId();
        final TaskId parentTaskId = new TaskId(nodeId, task.getId());

        // note: when security is turned on, the indices are already resolved
        // TODO: do a quick check and only resolve if necessary??
        String[] concreteIndices = this.indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        Map<String, Set<ShardId>> nodesAndShards = resolveIndicesToPrimaryShards(clusterState, concreteIndices);
        if (nodesAndShards.isEmpty()) {
            listener.onResponse(new Response(Collections.emptyMap()));
            return;
        }

        if (request.getQuery() == null) {  // If there is no query, then there is no point in filtering
            getCheckpointsFromNodes(clusterState, task, nodesAndShards, new OriginalIndices(request), request.getTimeout(), listener);
            return;
        }

        SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
            request.indices(),
            SearchRequest.DEFAULT_INDICES_OPTIONS,
            request.getQuery(),
            null,
            null,
            false,
            request.getCluster()
        );
        searchShardsRequest.setParentTask(parentTaskId);
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.TRANSFORM_ORIGIN,
            TransportSearchShardsAction.TYPE,
            searchShardsRequest,
            ActionListener.wrap(searchShardsResponse -> {
                Map<String, Set<ShardId>> filteredNodesAndShards = filterOutSkippedShards(nodesAndShards, searchShardsResponse);
                getCheckpointsFromNodes(
                    clusterState,
                    task,
                    filteredNodesAndShards,
                    new OriginalIndices(request),
                    request.getTimeout(),
                    listener
                );
            }, e -> {
                // search_shards API failed so we just log the error here and continue just like there was no query
                logger.atWarn().withThrowable(e).log("search_shards API failed for cluster [{}]", request.getCluster());
                logger.atTrace()
                    .withThrowable(e)
                    .log("search_shards API failed for cluster [{}], request was [{}]", request.getCluster(), searchShardsRequest);
                getCheckpointsFromNodes(clusterState, task, nodesAndShards, new OriginalIndices(request), request.getTimeout(), listener);
            })
        );
    }

    private static Map<String, Set<ShardId>> resolveIndicesToPrimaryShards(ClusterState clusterState, String[] concreteIndices) {
        if (concreteIndices.length == 0) {
            return Collections.emptyMap();
        }

        final DiscoveryNodes nodes = clusterState.nodes();
        Map<String, Set<ShardId>> nodesAndShards = new HashMap<>();

        ShardsIterator shardsIt = clusterState.routingTable().allShards(concreteIndices);
        for (ShardRouting shard : shardsIt) {
            // only take primary shards, which should be exactly 1, this isn't strictly necessary
            // and we should consider taking any shard copy, but then we need another way to de-dup
            if (shard.primary() == false) {
                continue;
            }
            if (shard.assignedToNode() && nodes.get(shard.currentNodeId()) != null) {
                // special case: The minimum TransportVersion in the cluster is on an old version
                if (clusterState.getMinTransportVersion().before(TransportVersions.V_8_2_0)) {
                    throw new ActionNotFoundTransportException(GetCheckpointNodeAction.NAME);
                }

                String nodeId = shard.currentNodeId();
                nodesAndShards.computeIfAbsent(nodeId, k -> new HashSet<>()).add(shard.shardId());
            } else {
                throw new NoShardAvailableActionException(shard.shardId(), " no primary shards available for shard [" + shard + "]");
            }
        }
        return nodesAndShards;
    }

    static Map<String, Set<ShardId>> filterOutSkippedShards(
        Map<String, Set<ShardId>> nodesAndShards,
        SearchShardsResponse searchShardsResponse
    ) {
        Map<String, Set<ShardId>> filteredNodesAndShards = new HashMap<>(nodesAndShards.size());
        // Create a deep copy of the given nodes and shards map.
        for (Map.Entry<String, Set<ShardId>> nodeAndShardsEntry : nodesAndShards.entrySet()) {
            String node = nodeAndShardsEntry.getKey();
            Set<ShardId> shards = nodeAndShardsEntry.getValue();
            filteredNodesAndShards.put(node, new HashSet<>(shards));
        }
        // Remove (node, shard) pairs for all the skipped shards.
        for (SearchShardsGroup shardGroup : searchShardsResponse.getGroups()) {
            if (shardGroup.skipped()) {
                for (String allocatedNode : shardGroup.allocatedNodes()) {
                    Set<ShardId> shards = filteredNodesAndShards.get(allocatedNode);
                    if (shards != null) {
                        shards.remove(shardGroup.shardId());
                        if (shards.isEmpty()) {
                            // Remove node if no shards were left.
                            filteredNodesAndShards.remove(allocatedNode);
                        }
                    }
                }
            }
        }
        return filteredNodesAndShards;
    }

    private void getCheckpointsFromNodes(
        ClusterState clusterState,
        Task task,
        Map<String, Set<ShardId>> nodesAndShards,
        OriginalIndices originalIndices,
        TimeValue timeout,
        ActionListener<Response> listener
    ) {
        if (nodesAndShards.isEmpty()) {
            listener.onResponse(new Response(Map.of()));
            return;
        }

        final String localNodeId = clusterService.localNode().getId();

        GroupedActionListener<GetCheckpointNodeAction.Response> groupedListener = new GroupedActionListener<>(
            nodesAndShards.size(),
            ActionListener.wrap(responses -> listener.onResponse(mergeNodeResponses(responses)), listener::onFailure)
        );

        for (Entry<String, Set<ShardId>> oneNodeAndItsShards : nodesAndShards.entrySet()) {
            if (task instanceof CancellableTask) {
                // There is no point continuing this work if the task has been cancelled.
                if (((CancellableTask) task).notifyIfCancelled(listener)) {
                    return;
                }
            }
            if (localNodeId.equals(oneNodeAndItsShards.getKey())) {
                TransportGetCheckpointNodeAction.getGlobalCheckpoints(
                    indicesService,
                    task,
                    oneNodeAndItsShards.getValue(),
                    timeout,
                    Clock.systemUTC(),
                    groupedListener
                );
                continue;
            }

            DiscoveryNodes nodes = clusterState.nodes();
            DiscoveryNode node = nodes.get(oneNodeAndItsShards.getKey());

            // paranoia: this should not be possible using the same cluster state
            if (node == null) {
                listener.onFailure(
                    new UnavailableShardsException(
                        oneNodeAndItsShards.getValue().iterator().next(),
                        "Node not found for [{}] shards",
                        oneNodeAndItsShards.getValue().size()
                    )
                );
                return;
            }

            logger.trace("get checkpoints from node {}", node);
            GetCheckpointNodeAction.Request nodeCheckpointsRequest = new GetCheckpointNodeAction.Request(
                oneNodeAndItsShards.getValue(),
                originalIndices,
                timeout
            );
            transportService.sendChildRequest(
                node,
                GetCheckpointNodeAction.NAME,
                nodeCheckpointsRequest,
                task,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(
                    groupedListener,
                    GetCheckpointNodeAction.Response::new,
                    TransportResponseHandler.TRANSPORT_WORKER
                )
            );
        }
    }

    private static Response mergeNodeResponses(Collection<GetCheckpointNodeAction.Response> responses) {
        // the final list should be ordered by key
        Map<String, long[]> checkpointsByIndexReduced = new TreeMap<>();

        // merge the node responses
        for (GetCheckpointNodeAction.Response response : responses) {
            response.getCheckpoints().forEach((index, checkpoint) -> {
                if (checkpointsByIndexReduced.containsKey(index)) {
                    long[] shardCheckpoints = checkpointsByIndexReduced.get(index);
                    for (int i = 0; i < checkpoint.length; ++i) {
                        shardCheckpoints[i] = Math.max(shardCheckpoints[i], checkpoint[i]);
                    }
                } else {
                    checkpointsByIndexReduced.put(index, checkpoint);
                }
            });
        }

        return new Response(checkpointsByIndexReduced);
    }
}
