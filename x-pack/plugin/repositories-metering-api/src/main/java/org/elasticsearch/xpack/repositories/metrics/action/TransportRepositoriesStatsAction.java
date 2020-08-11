/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.metrics.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public final class TransportRepositoriesStatsAction extends TransportNodesAction<
    RepositoriesMetricsRequest,
    RepositoriesMetricsResponse,
    TransportRepositoriesStatsAction.RepositoriesNodeStatsRequest,
    RepositoriesNodeMetricsResponse> {

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportRepositoriesStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        RepositoriesService repositoriesService
    ) {
        super(
            RepositoriesMetricsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            RepositoriesMetricsRequest::new,
            RepositoriesNodeStatsRequest::new,
            ThreadPool.Names.SAME,
            RepositoriesNodeMetricsResponse.class
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected RepositoriesMetricsResponse newResponse(
        RepositoriesMetricsRequest request,
        List<RepositoriesNodeMetricsResponse> repositoriesNodeStatsResponses,
        List<FailedNodeException> failures
    ) {
        return new RepositoriesMetricsResponse(clusterService.getClusterName(), repositoriesNodeStatsResponses, failures);
    }

    @Override
    protected RepositoriesNodeStatsRequest newNodeRequest(RepositoriesMetricsRequest request) {
        return new RepositoriesNodeStatsRequest();
    }

    @Override
    protected RepositoriesNodeMetricsResponse newNodeResponse(StreamInput in) throws IOException {
        return new RepositoriesNodeMetricsResponse(in);
    }

    @Override
    protected RepositoriesNodeMetricsResponse nodeOperation(RepositoriesNodeStatsRequest request, Task task) {
        return new RepositoriesNodeMetricsResponse(clusterService.localNode(), repositoriesService.repositoriesStats());
    }

    static final class RepositoriesNodeStatsRequest extends TransportRequest {
        RepositoriesNodeStatsRequest() {}

        RepositoriesNodeStatsRequest(StreamInput in) throws IOException {
            super(in);
        }
    }
}
