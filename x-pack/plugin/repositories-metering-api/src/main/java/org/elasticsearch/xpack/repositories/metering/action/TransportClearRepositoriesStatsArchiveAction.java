/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryStatsSnapshot;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public final class TransportClearRepositoriesStatsArchiveAction extends TransportNodesAction<
    ClearRepositoriesMeteringArchiveRequest,
    RepositoriesMeteringResponse,
    TransportClearRepositoriesStatsArchiveAction.ClearRepositoriesStatsArchiveNodeRequest,
    RepositoriesNodeMeteringResponse> {

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
            ClearRepositoriesMeteringArchiveAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            ClearRepositoriesStatsArchiveNodeRequest::new,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected RepositoriesMeteringResponse newResponse(
        ClearRepositoriesMeteringArchiveRequest request,
        List<RepositoriesNodeMeteringResponse> nodesResponses,
        List<FailedNodeException> failures
    ) {
        return new RepositoriesMeteringResponse(clusterService.getClusterName(), nodesResponses, failures);
    }

    @Override
    protected ClearRepositoriesStatsArchiveNodeRequest newNodeRequest(ClearRepositoriesMeteringArchiveRequest request) {
        return new ClearRepositoriesStatsArchiveNodeRequest(request.getMaxVersionToClear());
    }

    @Override
    protected RepositoriesNodeMeteringResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new RepositoriesNodeMeteringResponse(in);
    }

    @Override
    protected RepositoriesNodeMeteringResponse nodeOperation(ClearRepositoriesStatsArchiveNodeRequest request, Task task) {
        List<RepositoryStatsSnapshot> clearedStats = repositoriesService.clearRepositoriesStatsArchive(request.maxVersionToClear);
        return new RepositoriesNodeMeteringResponse(clusterService.localNode(), clearedStats);
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
