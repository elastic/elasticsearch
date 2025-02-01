/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalClusterStateAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutingPolicy;
import org.elasticsearch.xpack.enrich.EnrichPolicyExecutor;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TransportEnrichStatsAction extends TransportLocalClusterStateAction<EnrichStatsAction.Request, EnrichStatsAction.Response> {

    private final Client client;

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportEnrichStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            EnrichStatsAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            EnrichStatsAction.Request::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        EnrichStatsAction.Request request,
        ClusterState state,
        ActionListener<EnrichStatsAction.Response> listener
    ) {
        EnrichCoordinatorStatsAction.Request statsRequest = new EnrichCoordinatorStatsAction.Request();
        ActionListener<EnrichCoordinatorStatsAction.Response> statsListener = listener.delegateFailureAndWrap((delegate, response) -> {
            if (response.hasFailures()) {
                // Report failures even if some node level requests succeed:
                Exception failure = null;
                for (FailedNodeException nodeFailure : response.failures()) {
                    if (failure == null) {
                        failure = nodeFailure;
                    } else {
                        failure.addSuppressed(nodeFailure);
                    }
                }
                delegate.onFailure(failure);
                return;
            }

            List<CoordinatorStats> coordinatorStats = response.getNodes()
                .stream()
                .map(EnrichCoordinatorStatsAction.NodeResponse::getCoordinatorStats)
                .sorted(Comparator.comparing(CoordinatorStats::nodeId))
                .collect(Collectors.toList());
            List<ExecutingPolicy> policyExecutionTasks = taskManager.getTasks()
                .values()
                .stream()
                .filter(t -> t.getAction().equals(EnrichPolicyExecutor.TASK_ACTION))
                .map(t -> t.taskInfo(clusterService.localNode().getId(), true))
                .map(t -> new ExecutingPolicy(t.description(), t))
                .sorted(Comparator.comparing(ExecutingPolicy::name))
                .collect(Collectors.toList());
            List<EnrichStatsAction.Response.CacheStats> cacheStats = response.getNodes()
                .stream()
                .map(EnrichCoordinatorStatsAction.NodeResponse::getCacheStats)
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(EnrichStatsAction.Response.CacheStats::nodeId))
                .collect(Collectors.toList());
            delegate.onResponse(new EnrichStatsAction.Response(policyExecutionTasks, coordinatorStats, cacheStats));
        });
        client.execute(EnrichCoordinatorStatsAction.INSTANCE, statsRequest, statsListener);
    }

    @Override
    protected ClusterBlockException checkBlock(EnrichStatsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
