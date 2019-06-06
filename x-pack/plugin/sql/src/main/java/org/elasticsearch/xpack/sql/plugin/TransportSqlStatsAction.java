/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;

import java.util.List;

/**
 * Performs the stats operation.
 */
public class TransportSqlStatsAction extends TransportNodesAction<SqlStatsRequest, SqlStatsResponse,
        SqlStatsRequest.NodeStatsRequest, SqlStatsResponse.NodeStatsResponse> {
    
    // the plan executor holds the metrics
    private final PlanExecutor planExecutor;

    @Inject
    public TransportSqlStatsAction(TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters, PlanExecutor planExecutor) {
        super(SqlStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
              SqlStatsRequest::new, SqlStatsRequest.NodeStatsRequest::new, ThreadPool.Names.MANAGEMENT,
              SqlStatsResponse.NodeStatsResponse.class);
        this.planExecutor = planExecutor;
    }

    @Override
    protected SqlStatsResponse newResponse(SqlStatsRequest request, List<SqlStatsResponse.NodeStatsResponse> nodes,
                                           List<FailedNodeException> failures) {
        return new SqlStatsResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected SqlStatsRequest.NodeStatsRequest newNodeRequest(String nodeId, SqlStatsRequest request) {
        return new SqlStatsRequest.NodeStatsRequest(request, nodeId);
    }

    @Override
    protected SqlStatsResponse.NodeStatsResponse newNodeResponse() {
        return new SqlStatsResponse.NodeStatsResponse();
    }

    @Override
    protected SqlStatsResponse.NodeStatsResponse nodeOperation(SqlStatsRequest.NodeStatsRequest request) {
        SqlStatsResponse.NodeStatsResponse statsResponse = new SqlStatsResponse.NodeStatsResponse(clusterService.localNode());
        statsResponse.setStats(planExecutor.metrics().stats());
        
        return statsResponse;
    }
}
