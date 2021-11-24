/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Response;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class TransportGetCheckpointAction extends HandledTransportAction<Request, Response> {

    private final Client client;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportGetCheckpointAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final Client client,
        final ClusterService clusterService,
        final IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(GetCheckpointAction.NAME, transportService, actionFilters, Request::new);
        this.client = client;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState state = clusterService.state();

        // TODO: does this action respect user headers?
        String[] concreteIndices = this.indexNameExpressionResolver.concreteIndexNames(
            state,
            request.indicesOptions(),
            true, // includeDataStreams
            request.indices()
        );

        new AsyncGetCheckpointsFromNodesAction(task, resolveIndicesToPrimaryShards(state, concreteIndices), listener).start();
    }

    private Map<String, Set<ShardId>> resolveIndicesToPrimaryShards(ClusterState state, String[] concreteIndices) {
        final DiscoveryNodes nodes = state.nodes();
        Map<String, Set<ShardId>> nodesAndShards = new HashMap<>();

        ShardsIterator shardsIt = state.routingTable().allShards(concreteIndices);
        for (ShardRouting shard : shardsIt) {
            // only take primary shards, which should be exactly 1, this isn't strictly necessary
            // and we should consider taking any shard copy, but than we need another way to de-dup
            if (shard.primary() && shard.assignedToNode() && nodes.get(shard.currentNodeId()) != null) {
                String nodeId = shard.currentNodeId();
                nodesAndShards.computeIfAbsent(nodeId, k -> new HashSet<>()).add(shard.shardId());
            }
        }
        return nodesAndShards;
    }

    protected class AsyncGetCheckpointsFromNodesAction {
        private final Task task;
        private final ActionListener<Response> listener;
        private final Map<String, Set<ShardId>> nodesAndShards;

        protected AsyncGetCheckpointsFromNodesAction(
            Task task,
            Map<String, Set<ShardId>> nodesAndShards,
            ActionListener<Response> listener
        ) {
            this.task = task;
            this.listener = listener;
            this.nodesAndShards = nodesAndShards;
        }

        public void start() {

            GroupedActionListener<GetCheckpointNodeAction.Response> groupedListener = new GroupedActionListener<>(
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
                }, listener::onFailure),
                nodesAndShards.size()
            );

            for (Entry<String, Set<ShardId>> oneNodeAndItsShards : nodesAndShards.entrySet()) {
                GetCheckpointNodeAction.Request nodeCheckpointsRequest = new GetCheckpointNodeAction.Request(
                    oneNodeAndItsShards.getValue()
                );
                nodeCheckpointsRequest.setParentTask(clusterService.localNode().getId(), task.getId());

                // TODO: shortcut if shard is on the local node

                // execute it with transform origin
                ClientHelper.executeAsyncWithOrigin(
                    client,
                    ClientHelper.TRANSFORM_ORIGIN,
                    GetCheckpointNodeAction.INSTANCE,
                    nodeCheckpointsRequest,
                    groupedListener
                );
            }
        }
    }
}
