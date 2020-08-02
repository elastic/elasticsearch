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

public final class TransportClearRepositoriesStatsArchiveAction extends TransportNodesAction<
    ClearRepositoriesStatsArchiveRequest,
    ClearRepositoriesStatsArchiveResponse,
    ClearRepositoriesStatsArchiveNodeRequest,
    ClearRepositoriesStatsArchiveNodeResponse> {

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportClearRepositoriesStatsArchiveAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        RepositoriesService repositoriesService
    ) {
        super(
            ClearRepositoriesStatsArchiveAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ClearRepositoriesStatsArchiveRequest::new,
            ClearRepositoriesStatsArchiveNodeRequest::new,
            ThreadPool.Names.SAME,
            ClearRepositoriesStatsArchiveNodeResponse.class
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected ClearRepositoriesStatsArchiveResponse newResponse(
        ClearRepositoriesStatsArchiveRequest request,
        List<ClearRepositoriesStatsArchiveNodeResponse> nodesResponses,
        List<FailedNodeException> failures
    ) {
        return new ClearRepositoriesStatsArchiveResponse(clusterService.getClusterName(), nodesResponses, failures);
    }

    @Override
    protected ClearRepositoriesStatsArchiveNodeRequest newNodeRequest(ClearRepositoriesStatsArchiveRequest request) {
        return new ClearRepositoriesStatsArchiveNodeRequest();
    }

    @Override
    protected ClearRepositoriesStatsArchiveNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ClearRepositoriesStatsArchiveNodeResponse(in);
    }

    @Override
    protected ClearRepositoriesStatsArchiveNodeResponse nodeOperation(ClearRepositoriesStatsArchiveNodeRequest request, Task task) {
        repositoriesService.clearRepositoriesStatsArchive();
        return new ClearRepositoriesStatsArchiveNodeResponse(clusterService.localNode());
    }
}
