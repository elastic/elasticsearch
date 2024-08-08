/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.verify;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for verifying repository operation
 */
public class TransportVerifyRepositoryAction extends TransportMasterNodeAction<VerifyRepositoryRequest, VerifyRepositoryResponse> {

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportVerifyRepositoryAction(
        TransportService transportService,
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            VerifyRepositoryAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            VerifyRepositoryRequest::new,
            indexNameExpressionResolver,
            VerifyRepositoryResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected ClusterBlockException checkBlock(VerifyRepositoryRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final VerifyRepositoryRequest request,
        ClusterState state,
        final ActionListener<VerifyRepositoryResponse> listener
    ) {
        repositoriesService.verifyRepository(
            request.name(),
            listener.map(verifyResponse -> new VerifyRepositoryResponse(verifyResponse.toArray(new DiscoveryNode[0])))
        );
    }
}
