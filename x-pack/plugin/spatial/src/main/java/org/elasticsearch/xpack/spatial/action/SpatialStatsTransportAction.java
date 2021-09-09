/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.spatial.action.SpatialStatsAction;
import org.elasticsearch.xpack.spatial.SpatialUsage;

import java.io.IOException;
import java.util.List;

public class SpatialStatsTransportAction extends TransportNodesAction<SpatialStatsAction.Request, SpatialStatsAction.Response,
        SpatialStatsAction.NodeRequest, SpatialStatsAction.NodeResponse> {
    private final SpatialUsage usage;

    @Inject
    public SpatialStatsTransportAction(TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters, SpatialUsage usage) {
        super(SpatialStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
            SpatialStatsAction.Request::new, SpatialStatsAction.NodeRequest::new, ThreadPool.Names.MANAGEMENT,
            SpatialStatsAction.NodeResponse.class);
        this.usage = usage;
    }

    @Override
    protected SpatialStatsAction.Response newResponse(SpatialStatsAction.Request request,
                                                        List<SpatialStatsAction.NodeResponse> nodes,
                                                        List<FailedNodeException> failures) {
        return new SpatialStatsAction.Response(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected SpatialStatsAction.NodeRequest newNodeRequest(SpatialStatsAction.Request request) {
        return new SpatialStatsAction.NodeRequest(request);
    }

    @Override
    protected SpatialStatsAction.NodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new SpatialStatsAction.NodeResponse(in);
    }

    @Override
    protected SpatialStatsAction.NodeResponse nodeOperation(SpatialStatsAction.NodeRequest request, Task task) {
        return usage.stats(clusterService.localNode());
    }
}
