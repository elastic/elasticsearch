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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryStatsSnapshot;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public final class TransportClearRepositoriesStatsArchiveAction extends TransportNodesAction<
    ClearRepositoriesStatsArchiveRequest,
    RepositoriesStatsResponse,
    TransportClearRepositoriesStatsArchiveAction.ClearRepositoriesStatsArchiveNodeRequest,
    RepositoriesNodeStatsResponse> {

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
            RepositoriesNodeStatsResponse.class
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected RepositoriesStatsResponse newResponse(
        ClearRepositoriesStatsArchiveRequest request,
        List<RepositoriesNodeStatsResponse> nodesResponses,
        List<FailedNodeException> failures
    ) {
        return new RepositoriesStatsResponse(clusterService.getClusterName(), nodesResponses, failures);
    }

    @Override
    protected ClearRepositoriesStatsArchiveNodeRequest newNodeRequest(ClearRepositoriesStatsArchiveRequest request) {
        return new ClearRepositoriesStatsArchiveNodeRequest(request.getMaxVersionToClear());
    }

    @Override
    protected RepositoriesNodeStatsResponse newNodeResponse(StreamInput in) throws IOException {
        return new RepositoriesNodeStatsResponse(in);
    }

    @Override
    protected RepositoriesNodeStatsResponse nodeOperation(ClearRepositoriesStatsArchiveNodeRequest request, Task task) {
        List<RepositoryStatsSnapshot> clearedStats = repositoriesService.clearRepositoriesStatsArchive(request.maxVersionToClear);
        return new RepositoriesNodeStatsResponse(clusterService.localNode(), clearedStats);
    }

    static final class ClearRepositoriesStatsArchiveNodeRequest extends TransportRequest {
        private final long maxVersionToClear;

        ClearRepositoriesStatsArchiveNodeRequest(long maxVersionToClear) {
            this.maxVersionToClear = maxVersionToClear;
        }

        ClearRepositoriesStatsArchiveNodeRequest(StreamInput in) throws IOException {
            super(in);
            this.maxVersionToClear = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(maxVersionToClear);
        }
    }
}
