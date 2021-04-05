/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetShutdownStatusAction extends TransportMasterNodeAction<
    GetShutdownStatusAction.Request,
    GetShutdownStatusAction.Response> {
    @Inject
    public TransportGetShutdownStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetShutdownStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetShutdownStatusAction.Request::readFrom,
            indexNameExpressionResolver,
            GetShutdownStatusAction.Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        GetShutdownStatusAction.Request request,
        ClusterState state,
        ActionListener<GetShutdownStatusAction.Response> listener
    ) throws Exception {
        // TODO: implement me!
        listener.onResponse(new GetShutdownStatusAction.Response(null));
    }

    @Override
    protected ClusterBlockException checkBlock(GetShutdownStatusAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
