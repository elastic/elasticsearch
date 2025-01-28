/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;

import java.io.IOException;
import java.util.List;

/**
 * Performs the stats operation.
 */
public class TransportEsqlStatsAction extends TransportNodesAction<
    EsqlStatsRequest,
    EsqlStatsResponse,
    EsqlStatsRequest.NodeStatsRequest,
    EsqlStatsResponse.NodeStatsResponse,
    Void> {

    // the plan executor holds the metrics
    private final FeatureService featureService;
    private final PlanExecutor planExecutor;

    @Inject
    public TransportEsqlStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        FeatureService featureService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        PlanExecutor planExecutor
    ) {
        super(
            EsqlStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            EsqlStatsRequest.NodeStatsRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.featureService = featureService;
        this.planExecutor = planExecutor;
    }

    @Override
    protected DiscoveryNode[] resolveRequest(EsqlStatsRequest request, ClusterState clusterState) {
        return super.resolveRequest(request, clusterState);
    }

    @Override
    protected EsqlStatsResponse newResponse(
        EsqlStatsRequest request,
        List<EsqlStatsResponse.NodeStatsResponse> nodes,
        List<FailedNodeException> failures
    ) {
        return new EsqlStatsResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected EsqlStatsRequest.NodeStatsRequest newNodeRequest(EsqlStatsRequest request) {
        return new EsqlStatsRequest.NodeStatsRequest(request);
    }

    @Override
    protected EsqlStatsResponse.NodeStatsResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new EsqlStatsResponse.NodeStatsResponse(in);
    }

    @Override
    protected EsqlStatsResponse.NodeStatsResponse nodeOperation(EsqlStatsRequest.NodeStatsRequest request, Task task) {
        EsqlStatsResponse.NodeStatsResponse statsResponse = new EsqlStatsResponse.NodeStatsResponse(clusterService.localNode());
        statsResponse.setStats(planExecutor.metrics().stats());
        return statsResponse;
    }
}
