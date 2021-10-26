/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class DeletePipelineTransportAction extends AcknowledgedTransportMasterNodeAction<DeletePipelineRequest> {

    private final IngestService ingestService;

    @Inject
    public DeletePipelineTransportAction(ThreadPool threadPool, IngestService ingestService, TransportService transportService,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(DeletePipelineAction.NAME, transportService, ingestService.getClusterService(),
            threadPool, actionFilters, DeletePipelineRequest::new, indexNameExpressionResolver, ThreadPool.Names.SAME);
        this.ingestService = ingestService;
    }

    @Override
    protected void masterOperation(Task task, DeletePipelineRequest request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        ingestService.delete(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeletePipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
