/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;
import java.util.Set;

public class DeletePipelineTransportAction extends AcknowledgedTransportMasterNodeAction<DeletePipelineRequest> {

    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("cluster:admin/ingest/pipeline/delete");
    private final IngestService ingestService;

    @Inject
    public DeletePipelineTransportAction(
        ThreadPool threadPool,
        IngestService ingestService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            ingestService.getClusterService(),
            threadPool,
            actionFilters,
            DeletePipelineRequest::new,
            indexNameExpressionResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.ingestService = ingestService;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeletePipelineRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        ingestService.delete(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeletePipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedPipelineAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(DeletePipelineRequest request) {
        return Set.of(request.getId());
    }
}
