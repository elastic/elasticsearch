/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;

import java.io.IOException;
import java.util.List;

/**
 * Performs the stats operation.
 */
public class TransportEqlStatsAction extends TransportNodesAction<
    EqlStatsRequest,
    EqlStatsResponse,
    EqlStatsRequest.NodeStatsRequest,
    EqlStatsResponse.NodeStatsResponse> {

    // the plan executor holds the metrics
    private final PlanExecutor planExecutor;

    @Inject
    public TransportEqlStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        PlanExecutor planExecutor
    ) {
        super(
            EqlStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            EqlStatsRequest.NodeStatsRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.planExecutor = planExecutor;
    }

    @Override
    protected EqlStatsResponse newResponse(
        EqlStatsRequest request,
        List<EqlStatsResponse.NodeStatsResponse> nodes,
        List<FailedNodeException> failures
    ) {
        return new EqlStatsResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected EqlStatsRequest.NodeStatsRequest newNodeRequest(EqlStatsRequest request) {
        return new EqlStatsRequest.NodeStatsRequest(request);
    }

    @Override
    protected EqlStatsResponse.NodeStatsResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new EqlStatsResponse.NodeStatsResponse(in);
    }

    @Override
    protected EqlStatsResponse.NodeStatsResponse nodeOperation(EqlStatsRequest.NodeStatsRequest request, Task task) {
        EqlStatsResponse.NodeStatsResponse statsResponse = new EqlStatsResponse.NodeStatsResponse(clusterService.localNode());
        statsResponse.setStats(planExecutor.metrics().stats());
        return statsResponse;
    }
}
