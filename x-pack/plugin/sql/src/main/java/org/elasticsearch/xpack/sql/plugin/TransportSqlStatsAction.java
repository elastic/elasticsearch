/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

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
import org.elasticsearch.xpack.sql.execution.PlanExecutor;

import java.io.IOException;
import java.util.List;

/**
 * Performs the stats operation.
 */
public class TransportSqlStatsAction extends TransportNodesAction<
    SqlStatsRequest,
    SqlStatsResponse,
    SqlStatsRequest.NodeStatsRequest,
    SqlStatsResponse.NodeStatsResponse> {

    // the plan executor holds the metrics
    private final PlanExecutor planExecutor;

    @Inject
    public TransportSqlStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        PlanExecutor planExecutor
    ) {
        super(
            SqlStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            SqlStatsRequest::new,
            SqlStatsRequest.NodeStatsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.planExecutor = planExecutor;
    }

    @Override
    protected SqlStatsResponse newResponse(
        SqlStatsRequest request,
        List<SqlStatsResponse.NodeStatsResponse> nodes,
        List<FailedNodeException> failures
    ) {
        return new SqlStatsResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected SqlStatsRequest.NodeStatsRequest newNodeRequest(SqlStatsRequest request) {
        return new SqlStatsRequest.NodeStatsRequest(request);
    }

    @Override
    protected SqlStatsResponse.NodeStatsResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new SqlStatsResponse.NodeStatsResponse(in);
    }

    @Override
    protected SqlStatsResponse.NodeStatsResponse nodeOperation(SqlStatsRequest.NodeStatsRequest request, Task task) {
        SqlStatsResponse.NodeStatsResponse statsResponse = new SqlStatsResponse.NodeStatsResponse(clusterService.localNode());
        statsResponse.setStats(planExecutor.metrics().stats());

        return statsResponse;
    }
}
