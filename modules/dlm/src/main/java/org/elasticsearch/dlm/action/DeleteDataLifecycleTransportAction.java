/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.dlm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.DataStreamsActionUtil;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.dlm.ModifyDataLifecycleService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class DeleteDataLifecycleTransportAction extends AcknowledgedTransportMasterNodeAction<DeleteDataLifecycleAction.Request> {

    private final ModifyDataLifecycleService modifyDataLifecycleService;

    @Inject
    public DeleteDataLifecycleTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ModifyDataLifecycleService modifyDataLifecycleService
    ) {
        super(
            DeleteDataLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDataLifecycleAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.modifyDataLifecycleService = modifyDataLifecycleService;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDataLifecycleAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        List<String> dataStreamNames = DataStreamsActionUtil.getDataStreamNames(
            indexNameExpressionResolver,
            state,
            request.getNames(),
            request.indicesOptions()
        );
        modifyDataLifecycleService.removeLifecycle(dataStreamNames, request.ackTimeout(), listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDataLifecycleAction.Request request, ClusterState state) {
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (globalBlock != null) {
            return globalBlock;
        }
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }
}
