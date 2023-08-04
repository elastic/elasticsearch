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
    RepositoriesMeteringRequest,
    RepositoriesMeteringResponse,
    TransportRepositoriesStatsAction.RepositoriesNodeStatsRequest,
    RepositoriesNodeMeteringResponse> {

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
            RepositoriesMeteringAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            RepositoriesMeteringRequest::new,
            RepositoriesNodeStatsRequest::new,
            ThreadPool.Names.GENERIC
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected RepositoriesMeteringResponse newResponse(
        RepositoriesMeteringRequest request,
        List<RepositoriesNodeMeteringResponse> repositoriesNodeStatsResponses,
        List<FailedNodeException> failures
    ) {
        return new RepositoriesMeteringResponse(clusterService.getClusterName(), repositoriesNodeStatsResponses, failures);
    }

    @Override
    protected RepositoriesNodeStatsRequest newNodeRequest(RepositoriesMeteringRequest request) {
        return new RepositoriesNodeStatsRequest();
    }

    @Override
    protected RepositoriesNodeMeteringResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new RepositoriesNodeMeteringResponse(in);
    }

    @Override
    protected RepositoriesNodeMeteringResponse nodeOperation(RepositoriesNodeStatsRequest request, Task task) {
        return new RepositoriesNodeMeteringResponse(clusterService.localNode(), repositoriesService.repositoriesStats());
    }

    static final class RepositoriesNodeStatsRequest extends TransportRequest {
        RepositoriesNodeStatsRequest() {}

        RepositoriesNodeStatsRequest(StreamInput in) throws IOException {
            super(in);
        }
    }
}
