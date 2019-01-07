/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.AbstractAllocateAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransportClusterRerouteAction extends TransportMasterNodeAction<ClusterRerouteRequest, ClusterRerouteResponse> {

    private final AllocationService allocationService;

    @Inject
    public TransportClusterRerouteAction(TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, AllocationService allocationService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ClusterRerouteAction.NAME, transportService, clusterService, threadPool, actionFilters,
              indexNameExpressionResolver, ClusterRerouteRequest::new);
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterRerouteRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterRerouteResponse newResponse() {
        return new ClusterRerouteResponse();
    }

    @Override
    protected void masterOperation(final ClusterRerouteRequest request, final ClusterState state,
                                   final ActionListener<ClusterRerouteResponse> listener) {
        ActionListener<ClusterRerouteResponse> logWrapper = ActionListener.wrap(
            response -> {
                if (request.dryRun() == false) {
                    response.getExplanations().getYesDecisionMessages().forEach(logger::info);
                }
                listener.onResponse(response);
            },
            listener::onFailure
        );

        // Gather all stale primary allocation commands into a map indexed by the index name they correspond to
        // so we can check if the nodes they correspond to actually have any data for the shard
        Map<String, List<AbstractAllocateAllocationCommand>> stalePrimaryAllocations = request.getCommands().commands().stream()
            .filter(cmd -> cmd instanceof AllocateStalePrimaryAllocationCommand).collect(
                Collectors.toMap(
                    cmd -> ((AbstractAllocateAllocationCommand) cmd).index(),
                    cmd -> Collections.singletonList((AbstractAllocateAllocationCommand) cmd),
                    (existing, added) -> {
                        List<AbstractAllocateAllocationCommand> res = new ArrayList<>(existing.size() + added.size());
                        res.addAll(existing);
                        res.addAll(added);
                        return res;
                    }
                )
            );
        if (stalePrimaryAllocations.isEmpty()) {
            // We don't have any stale primary allocations, we simply execute the state update task for the requested allocations
            submitStateUpdate(request, logWrapper);
        } else {
            // We get the index shard store status for indices that we want to allocate stale primaries on first to fail requests
            // where there's no data for a given shard on a given node.
            transportService.sendRequest(transportService.getLocalNode(), IndicesShardStoresAction.NAME,
                new IndicesShardStoresRequest().indices(stalePrimaryAllocations.keySet().toArray(Strings.EMPTY_ARRAY)),
                new ActionListenerResponseHandler<>(
                    ActionListener.wrap(
                        response -> {
                            ImmutableOpenMap<String, ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>>> status =
                                response.getStoreStatuses();
                            Exception e = null;
                            for (Map.Entry<String, List<AbstractAllocateAllocationCommand>> entry : stalePrimaryAllocations.entrySet()) {
                                final String index = entry.getKey();
                                final ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>> indexStatus = status.get(index);
                                if (indexStatus == null) {
                                    e = ExceptionsHelper.useOrSuppress(e, new IndexNotFoundException(index));
                                } else {
                                    for (AbstractAllocateAllocationCommand command : entry.getValue()) {
                                        final List<IndicesShardStoresResponse.StoreStatus> shardStatus =
                                            indexStatus.get(command.shardId());
                                        if (shardStatus == null) {
                                            e = ExceptionsHelper.useOrSuppress(e, new IllegalArgumentException(
                                                "No data for shard [" + command.shardId() + "] of index [" + index + "] found on any node")
                                            );
                                        } else if (shardStatus.stream()
                                            .noneMatch(storeStatus -> {
                                                final DiscoveryNode node = storeStatus.getNode();
                                                final String nodeInCommand = command.node();
                                                return nodeInCommand.equals(node.getName()) || nodeInCommand.equals(node.getId());
                                            })) {
                                            e = ExceptionsHelper.useOrSuppress(e, new IllegalArgumentException(
                                                "No data for shard [" + command.shardId() + "] of index [" + index + "] found on node ["
                                                    + command.node() + ']'));
                                        }
                                    }
                                }
                            }
                            if (e == null) {
                                submitStateUpdate(request, logWrapper);
                            } else {
                                logWrapper.onFailure(e);
                            }
                        }, logWrapper::onFailure
                    ), IndicesShardStoresResponse::new));
        }
    }

    private void submitStateUpdate(final ClusterRerouteRequest request, final ActionListener<ClusterRerouteResponse> logWrapper) {
        clusterService.submitStateUpdateTask("cluster_reroute (api)",
            new ClusterRerouteResponseAckedClusterStateUpdateTask(logger, allocationService, request, logWrapper));
    }

    static class ClusterRerouteResponseAckedClusterStateUpdateTask extends AckedClusterStateUpdateTask<ClusterRerouteResponse> {

        private final ClusterRerouteRequest request;
        private final ActionListener<ClusterRerouteResponse> listener;
        private final Logger logger;
        private final AllocationService allocationService;
        private volatile ClusterState clusterStateToSend;
        private volatile RoutingExplanations explanations;

        ClusterRerouteResponseAckedClusterStateUpdateTask(Logger logger, AllocationService allocationService, ClusterRerouteRequest request,
                                                          ActionListener<ClusterRerouteResponse> listener) {
            super(Priority.IMMEDIATE, request, listener);
            this.request = request;
            this.listener = listener;
            this.logger = logger;
            this.allocationService = allocationService;
        }

        @Override
        protected ClusterRerouteResponse newResponse(boolean acknowledged) {
            return new ClusterRerouteResponse(acknowledged, clusterStateToSend, explanations);
        }

        @Override
        public void onAckTimeout() {
            listener.onResponse(new ClusterRerouteResponse(false, clusterStateToSend, new RoutingExplanations()));
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.debug(() -> new ParameterizedMessage("failed to perform [{}]", source), e);
            super.onFailure(source, e);
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            AllocationService.CommandsResult commandsResult =
                allocationService.reroute(currentState, request.getCommands(), request.explain(), request.isRetryFailed());
            clusterStateToSend = commandsResult.getClusterState();
            explanations = commandsResult.explanations();
            if (request.dryRun()) {
                return currentState;
            }
            return commandsResult.getClusterState();
        }
    }
}
