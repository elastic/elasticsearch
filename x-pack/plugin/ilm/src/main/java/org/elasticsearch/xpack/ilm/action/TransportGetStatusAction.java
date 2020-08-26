/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction.Request;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction.Response;

import java.io.IOException;

public class TransportGetStatusAction extends TransportMasterNodeAction<Request, Response> {

    @Inject
    public TransportGetStatusAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetStatusAction.NAME, transportService, clusterService, threadPool, actionFilters,
            Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected Response read(StreamInput in) throws IOException {
        return new Response(in);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        IndexLifecycleMetadata metadata = state.metadata().custom(IndexLifecycleMetadata.TYPE);
        final Response response;
        if (metadata == null) {
            // no need to actually install metadata just yet, but safe to say it is not stopped
            response = new Response(OperationMode.RUNNING);
        } else {
            response = new Response(metadata.getOperationMode());
        }
        listener.onResponse(response);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
