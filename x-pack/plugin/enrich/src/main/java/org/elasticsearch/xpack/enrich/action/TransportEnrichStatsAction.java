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
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutingPolicy;
import org.elasticsearch.xpack.enrich.EnrichPolicyExecutor;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TransportEnrichStatsAction extends TransportMasterNodeAction<EnrichStatsAction.Request, EnrichStatsAction.Response> {

    private final Client client;

    @Inject
    public TransportEnrichStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            EnrichStatsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            EnrichStatsAction.Request::new,
            indexNameExpressionResolver,
            EnrichStatsAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.client = client;
    }

    @Override
    protected void masterOperation(
        Task task,
        EnrichStatsAction.Request request,
        ClusterState state,
        ActionListener<EnrichStatsAction.Response> listener
    ) throws Exception {
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
                .sorted(Comparator.comparing(CoordinatorStats::getNodeId))
                .collect(Collectors.toList());
            List<ExecutingPolicy> policyExecutionTasks = taskManager.getTasks()
                .values()
                .stream()
                .filter(t -> t.getAction().equals(EnrichPolicyExecutor.TASK_ACTION))
                .map(t -> t.taskInfo(clusterService.localNode().getId(), true))
                .map(t -> new ExecutingPolicy(t.description(), t))
                .sorted(Comparator.comparing(ExecutingPolicy::getName))
                .collect(Collectors.toList());
            List<EnrichStatsAction.Response.CacheStats> cacheStats = response.getNodes()
                .stream()
                .map(EnrichCoordinatorStatsAction.NodeResponse::getCacheStats)
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(EnrichStatsAction.Response.CacheStats::getNodeId))
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
