/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Response;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction;

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

    @Inject
    public TransportGetCheckpointAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final IndicesService indicesService,
        final ClusterService clusterService,
        final IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(GetCheckpointAction.NAME, transportService, actionFilters, Request::new);
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState state = clusterService.state();
        resolveIndicesAndGetCheckpoint(task, request, listener, state);
    }

    protected void resolveIndicesAndGetCheckpoint(Task task, Request request, ActionListener<Response> listener, final ClusterState state) {
        // note: when security is turned on, the indices are already resolved
        // TODO: do a quick check and only resolve if necessary??
        String[] concreteIndices = this.indexNameExpressionResolver.concreteIndexNames(state, request);

        Map<String, Set<ShardId>> nodesAndShards = resolveIndicesToPrimaryShards(state, concreteIndices);

        if (nodesAndShards.size() == 0) {
            listener.onResponse(new Response(Collections.emptyMap()));
            return;
        }

        new AsyncGetCheckpointsFromNodesAction(state, task, nodesAndShards, new OriginalIndices(request), listener).start();
    }

    private static Map<String, Set<ShardId>> resolveIndicesToPrimaryShards(ClusterState state, String[] concreteIndices) {
        if (concreteIndices.length == 0) {
            return Collections.emptyMap();
        }

        final DiscoveryNodes nodes = state.nodes();
        Map<String, Set<ShardId>> nodesAndShards = new HashMap<>();

        ShardsIterator shardsIt = state.routingTable().allShards(concreteIndices);
        for (ShardRouting shard : shardsIt) {
            // only take primary shards, which should be exactly 1, this isn't strictly necessary
            // and we should consider taking any shard copy, but then we need another way to de-dup
            if (shard.primary() == false) {
                continue;
            }
            if (shard.assignedToNode() && nodes.get(shard.currentNodeId()) != null) {
                // special case: a node that holds the shard is on an old version
                if (nodes.get(shard.currentNodeId()).getVersion().before(Version.V_8_2_0)) {
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

    protected class AsyncGetCheckpointsFromNodesAction {
        private final Task task;
        private final ActionListener<Response> listener;
        private final Map<String, Set<ShardId>> nodesAndShards;
        private final OriginalIndices originalIndices;
        private final DiscoveryNodes nodes;
        private final String localNodeId;

        protected AsyncGetCheckpointsFromNodesAction(
            ClusterState clusterState,
            Task task,
            Map<String, Set<ShardId>> nodesAndShards,
            OriginalIndices originalIndices,
            ActionListener<Response> listener
        ) {
            this.task = task;
            this.listener = listener;
            this.nodesAndShards = nodesAndShards;
            this.originalIndices = originalIndices;
            this.nodes = clusterState.nodes();
            this.localNodeId = clusterService.localNode().getId();
        }

        public void start() {
            GroupedActionListener<GetCheckpointNodeAction.Response> groupedListener = new GroupedActionListener<>(
                nodesAndShards.size(),
                ActionListener.wrap(responses -> {
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

                    listener.onResponse(new Response(checkpointsByIndexReduced));
                }, listener::onFailure)
            );

            for (Entry<String, Set<ShardId>> oneNodeAndItsShards : nodesAndShards.entrySet()) {
                if (localNodeId.equals(oneNodeAndItsShards.getKey())) {
                    TransportGetCheckpointNodeAction.getGlobalCheckpoints(indicesService, oneNodeAndItsShards.getValue(), groupedListener);
                    continue;
                }

                GetCheckpointNodeAction.Request nodeCheckpointsRequest = new GetCheckpointNodeAction.Request(
                    oneNodeAndItsShards.getValue(),
                    originalIndices
                );
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
                transportService.sendChildRequest(
                    node,
                    GetCheckpointNodeAction.NAME,
                    nodeCheckpointsRequest,
                    task,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(groupedListener, GetCheckpointNodeAction.Response::new)
                );
            }
        }
    }
}
