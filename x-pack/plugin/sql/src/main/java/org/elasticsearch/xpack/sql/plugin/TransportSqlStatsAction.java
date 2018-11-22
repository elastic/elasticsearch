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
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.Arrays;
import java.util.List;

/**
 * Performs the stats operation.
 */
public class TransportSqlStatsAction extends TransportNodesAction<SqlStatsRequest, SqlStatsResponse,
        SqlStatsRequest.Node, SqlStatsResponse.Node> {
    
    private final TransportSqlTranslateAction translateAction;
    private final TransportSqlQueryAction queryAction;

    @Inject
    public TransportSqlStatsAction(TransportService transportService, ClusterService clusterService,
            TransportSqlTranslateAction translateAction, TransportSqlQueryAction queryAction,
            ThreadPool threadPool, ActionFilters actionFilters) {
        super(SqlStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
                SqlStatsRequest::new, SqlStatsRequest.Node::new, ThreadPool.Names.MANAGEMENT, SqlStatsResponse.Node.class);
        this.translateAction = translateAction;
        this.queryAction = queryAction;
    }

    @Override
    protected SqlStatsResponse newResponse(SqlStatsRequest request, List<SqlStatsResponse.Node> nodes,
                                               List<FailedNodeException> failures) {
        return new SqlStatsResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected SqlStatsRequest.Node newNodeRequest(String nodeId, SqlStatsRequest request) {
        return new SqlStatsRequest.Node(request, nodeId);
    }

    @Override
    protected SqlStatsResponse.Node newNodeResponse() {
        return new SqlStatsResponse.Node();
    }

    @Override
    protected SqlStatsResponse.Node nodeOperation(SqlStatsRequest.Node request) {
        SqlStatsResponse.Node statsResponse = new SqlStatsResponse.Node(clusterService.localNode());
        Counters stats = Counters.merge(Arrays.asList(translateAction.stats(), queryAction.stats()));
        statsResponse.setStats(stats);
        
        return statsResponse;
    }
}
