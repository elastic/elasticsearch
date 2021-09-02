/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.analytics.AnalyticsUsage;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;

import java.io.IOException;
import java.util.List;

public class TransportAnalyticsStatsAction extends TransportNodesAction<
    AnalyticsStatsAction.Request,
    AnalyticsStatsAction.Response,
    AnalyticsStatsAction.NodeRequest,
    AnalyticsStatsAction.NodeResponse> {
    private final AnalyticsUsage usage;

    @Inject
    public TransportAnalyticsStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        AnalyticsUsage usage
    ) {
        super(
            AnalyticsStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            AnalyticsStatsAction.Request::new,
            AnalyticsStatsAction.NodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            AnalyticsStatsAction.NodeResponse.class
        );
        this.usage = usage;
    }

    @Override
    protected AnalyticsStatsAction.Response newResponse(
        AnalyticsStatsAction.Request request,
        List<AnalyticsStatsAction.NodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        return new AnalyticsStatsAction.Response(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected AnalyticsStatsAction.NodeRequest newNodeRequest(AnalyticsStatsAction.Request request) {
        return new AnalyticsStatsAction.NodeRequest(request);
    }

    @Override
    protected AnalyticsStatsAction.NodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new AnalyticsStatsAction.NodeResponse(in);
    }

    @Override
    protected AnalyticsStatsAction.NodeResponse nodeOperation(AnalyticsStatsAction.NodeRequest request, Task task) {
        return usage.stats(clusterService.localNode());
    }

}
