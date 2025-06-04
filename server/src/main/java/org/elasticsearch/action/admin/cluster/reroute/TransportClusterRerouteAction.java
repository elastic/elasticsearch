/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener;
import org.elasticsearch.cluster.routing.allocation.command.AbstractAllocateAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportClusterRerouteAction extends TransportMasterNodeAction<ClusterRerouteRequest, ClusterRerouteResponse> {

    public static final ActionType<ClusterRerouteResponse> TYPE = new ActionType<>("cluster:admin/reroute");
    private static final Logger logger = LogManager.getLogger(TransportClusterRerouteAction.class);

    private final AllocationService allocationService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportClusterRerouteAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        AllocationService allocationService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterRerouteRequest::new,
            ClusterRerouteResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.allocationService = allocationService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterRerouteRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        final ClusterRerouteRequest request,
        final ClusterState state,
        final ActionListener<ClusterRerouteResponse> listener
    ) {
        validateRequest(request);
        Map<String, List<AbstractAllocateAllocationCommand>> stalePrimaryAllocations = new HashMap<>();
        for (AllocationCommand command : request.getCommands().commands()) {
            if (command instanceof final AllocateStalePrimaryAllocationCommand cmd) {
                stalePrimaryAllocations.computeIfAbsent(cmd.index(), k -> new ArrayList<>()).add(cmd);
            }
        }
        if (stalePrimaryAllocations.isEmpty()) {
            submitStateUpdate(request, listener);
        } else {
            verifyThenSubmitUpdate(request, listener, stalePrimaryAllocations);
        }
    }

    @FixForMultiProject(description = "consider moving the validation to ActionRequest#validate")
    private void validateRequest(ClusterRerouteRequest request) {
        if (projectResolver.supportsMultipleProjects() && request.isRetryFailed() && request.getCommands().commands().isEmpty() == false) {
            throw new IllegalArgumentException(
                "cannot use 'retry_failed' query parameter with allocation-commands together in a cluster that supports multiple projects."
            );
        }

        final ProjectId projectId = projectResolver.getProjectId();
        if (request.getCommands().commands().stream().allMatch(command -> projectId.equals(command.projectId())) == false) {
            final String message = "inconsistent project-id: expected "
                + projectId
                + " but got "
                + request.getCommands().commands().stream().map(AllocationCommand::projectId).toList();
            throw new IllegalStateException(message);
        }
    }

    private void verifyThenSubmitUpdate(
        ClusterRerouteRequest request,
        ActionListener<ClusterRerouteResponse> listener,
        Map<String, List<AbstractAllocateAllocationCommand>> stalePrimaryAllocations
    ) {
        transportService.sendRequest(
            transportService.getLocalNode(),
            TransportIndicesShardStoresAction.TYPE.name(),
            new IndicesShardStoresRequest().indices(stalePrimaryAllocations.keySet().toArray(Strings.EMPTY_ARRAY)),
            new ActionListenerResponseHandler<>(listener.delegateFailureAndWrap((delegate, response) -> {
                Map<String, Map<Integer, List<IndicesShardStoresResponse.StoreStatus>>> status = response.getStoreStatuses();
                Exception e = null;
                for (Map.Entry<String, List<AbstractAllocateAllocationCommand>> entry : stalePrimaryAllocations.entrySet()) {
                    final String index = entry.getKey();
                    final Map<Integer, List<IndicesShardStoresResponse.StoreStatus>> indexStatus = status.get(index);
                    if (indexStatus == null) {
                        // The index in the stale primary allocation request was green and hence filtered out by the store status
                        // request. We ignore it here since the relevant exception will be thrown by the reroute action later on.
                        continue;
                    }
                    for (AbstractAllocateAllocationCommand command : entry.getValue()) {
                        final List<IndicesShardStoresResponse.StoreStatus> shardStatus = indexStatus.get(command.shardId());
                        if (shardStatus == null || shardStatus.isEmpty()) {
                            e = ExceptionsHelper.useOrSuppress(
                                e,
                                new IllegalArgumentException(
                                    "No data for shard [" + command.shardId() + "] of index [" + index + "] found on any node"
                                )
                            );
                        } else if (shardStatus.stream().noneMatch(storeStatus -> {
                            final DiscoveryNode node = storeStatus.getNode();
                            final String nodeInCommand = command.node();
                            return nodeInCommand.equals(node.getName()) || nodeInCommand.equals(node.getId());
                        })) {
                            e = ExceptionsHelper.useOrSuppress(
                                e,
                                new IllegalArgumentException(
                                    "No data for shard ["
                                        + command.shardId()
                                        + "] of index ["
                                        + index
                                        + "] found on node ["
                                        + command.node()
                                        + ']'
                                )
                            );
                        }
                    }
                }
                if (e == null) {
                    submitStateUpdate(request, delegate);
                } else {
                    delegate.onFailure(e);
                }
            }), IndicesShardStoresResponse::new, TransportResponseHandler.TRANSPORT_WORKER)
        );
    }

    private static final String TASK_SOURCE = "cluster_reroute (api)";

    private void submitStateUpdate(final ClusterRerouteRequest request, final ActionListener<ClusterRerouteResponse> listener) {
        submitUnbatchedTask(
            TASK_SOURCE,
            new ClusterRerouteResponseAckedClusterStateUpdateTask(
                logger,
                allocationService,
                threadPool.getThreadContext(),
                request,
                listener.map(response -> {
                    if (request.dryRun() == false) {
                        response.getExplanations().getYesDecisionMessages().forEach(logger::info);
                    }
                    return response;
                })
            )
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    static class ClusterRerouteResponseAckedClusterStateUpdateTask extends ClusterStateUpdateTask implements ClusterStateAckListener {

        private final ClusterRerouteRequest request;
        private final AllocationActionListener<ClusterRerouteResponse> listener;
        private final Logger logger;
        private final AllocationService allocationService;
        private volatile ClusterState clusterStateToSend;
        private volatile RoutingExplanations explanations;

        ClusterRerouteResponseAckedClusterStateUpdateTask(
            Logger logger,
            AllocationService allocationService,
            ThreadContext context,
            ClusterRerouteRequest request,
            ActionListener<ClusterRerouteResponse> listener
        ) {
            super(Priority.IMMEDIATE);
            this.request = request;
            this.listener = new AllocationActionListener<>(listener, context);
            this.logger = logger;
            this.allocationService = allocationService;
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public TimeValue ackTimeout() {
            return request.ackTimeout();
        }

        @Override
        public void onAllNodesAcked() {
            listener.clusterStateUpdate().onResponse(new ClusterRerouteResponse(true, clusterStateToSend, explanations));
        }

        @Override
        public void onAckFailure(Exception e) {
            listener.clusterStateUpdate().onResponse(new ClusterRerouteResponse(false, clusterStateToSend, explanations));
        }

        @Override
        public void onAckTimeout() {
            listener.clusterStateUpdate().onResponse(new ClusterRerouteResponse(false, clusterStateToSend, new RoutingExplanations()));
        }

        @Override
        public void onFailure(Exception e) {
            logger.debug("failed to perform [" + TASK_SOURCE + "]", e);
            listener.clusterStateUpdate().onFailure(e);
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            var result = allocationService.reroute(
                currentState,
                request.getCommands(),
                request.explain(),
                request.isRetryFailed(),
                request.dryRun(),
                listener.reroute()
            );
            clusterStateToSend = result.clusterState();
            explanations = result.explanations();
            return request.dryRun() ? currentState : result.clusterState();
        }
    }
}
