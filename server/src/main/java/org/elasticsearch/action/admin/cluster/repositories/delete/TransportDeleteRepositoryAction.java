/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.repositories.reservedstate.ReservedRepositoryAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;
import java.util.Set;

/**
 * Transport action for unregister repository operation
 */
public class TransportDeleteRepositoryAction extends AcknowledgedTransportMasterNodeAction<DeleteRepositoryRequest> {

    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("cluster:admin/repository/delete");
    private final RepositoriesService repositoriesService;

    @Inject
    public TransportDeleteRepositoryAction(
        TransportService transportService,
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteRepositoryRequest::new,
            indexNameExpressionResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteRepositoryRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        final DeleteRepositoryRequest request,
        ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        repositoriesService.unregisterRepository(request, listener);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedRepositoryAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(DeleteRepositoryRequest request) {
        return Set.of(request.name());
    }
}
