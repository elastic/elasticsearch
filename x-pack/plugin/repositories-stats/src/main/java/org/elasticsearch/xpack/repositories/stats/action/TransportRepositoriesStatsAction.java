/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public final class TransportRepositoriesStatsAction extends TransportNodesAction<
    RepositoriesStatsRequest,
    RepositoriesStatsResponse,
    RepositoriesNodeStatsRequest,
    RepositoriesNodeStatsResponse> {

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
            RepositoriesStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            RepositoriesStatsRequest::new,
            RepositoriesNodeStatsRequest::new,
            ThreadPool.Names.SAME,
            RepositoriesNodeStatsResponse.class
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected RepositoriesStatsResponse newResponse(
        RepositoriesStatsRequest request,
        List<RepositoriesNodeStatsResponse> repositoriesNodeStatsResponses,
        List<FailedNodeException> failures
    ) {
        return new RepositoriesStatsResponse(clusterService.getClusterName(), repositoriesNodeStatsResponses, failures);
    }

    @Override
    protected RepositoriesNodeStatsRequest newNodeRequest(RepositoriesStatsRequest request) {
        return new RepositoriesNodeStatsRequest();
    }

    @Override
    protected RepositoriesNodeStatsResponse newNodeResponse(StreamInput in) throws IOException {
        return new RepositoriesNodeStatsResponse(in);
    }

    @Override
    protected RepositoriesNodeStatsResponse nodeOperation(RepositoriesNodeStatsRequest request, Task task) {
        return new RepositoriesNodeStatsResponse(clusterService.localNode(), repositoriesService.repositoriesStats());
    }
}
