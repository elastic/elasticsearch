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
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class GetPipelineTransportAction extends TransportMasterNodeReadAction<GetPipelineRequest, GetPipelineResponse> {

    @Inject
    public GetPipelineTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetPipelineAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetPipelineRequest::new,
            indexNameExpressionResolver,
            GetPipelineResponse::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(Task task, GetPipelineRequest request, ClusterState state, ActionListener<GetPipelineResponse> listener)
        throws Exception {
        listener.onResponse(new GetPipelineResponse(IngestService.getPipelines(state, request.getIds()), request.isSummary()));
    }

    @Override
    protected ClusterBlockException checkBlock(GetPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

}
