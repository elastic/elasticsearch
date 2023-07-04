/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.health.stats;

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

import java.io.IOException;
import java.util.List;

/**
 * Performs the health api stats operation.
 */
public class HealthApiStatsTransportAction extends TransportNodesAction<
    HealthApiStatsAction.Request,
    HealthApiStatsAction.Response,
    HealthApiStatsAction.Request.Node,
    HealthApiStatsAction.Response.Node> {

    private final HealthApiStats healthApiStats;

    @Inject
    public HealthApiStatsTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        HealthApiStats healthApiStats
    ) {
        super(
            HealthApiStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            HealthApiStatsAction.Request::new,
            HealthApiStatsAction.Request.Node::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.healthApiStats = healthApiStats;
    }

    @Override
    protected HealthApiStatsAction.Response newResponse(
        HealthApiStatsAction.Request request,
        List<HealthApiStatsAction.Response.Node> nodes,
        List<FailedNodeException> failures
    ) {
        return new HealthApiStatsAction.Response(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected HealthApiStatsAction.Request.Node newNodeRequest(HealthApiStatsAction.Request request) {
        return new HealthApiStatsAction.Request.Node(request);
    }

    @Override
    protected HealthApiStatsAction.Response.Node newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new HealthApiStatsAction.Response.Node(in);
    }

    @Override
    protected HealthApiStatsAction.Response.Node nodeOperation(HealthApiStatsAction.Request.Node request, Task task) {
        HealthApiStatsAction.Response.Node statsResponse = new HealthApiStatsAction.Response.Node(clusterService.localNode());
        if (healthApiStats.hasCounters()) {
            statsResponse.setStats(healthApiStats.getStats());
        }
        return statsResponse;
    }
}
