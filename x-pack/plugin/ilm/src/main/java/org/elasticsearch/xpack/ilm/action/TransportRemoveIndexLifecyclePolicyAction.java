/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction.Request;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction.Response;
import org.elasticsearch.xpack.ilm.IndexLifecycleRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportRemoveIndexLifecyclePolicyAction extends TransportMasterNodeAction<Request, Response> {

    @Inject
    public TransportRemoveIndexLifecyclePolicyAction(TransportService transportService, ClusterService clusterService,
                                                     ThreadPool threadPool, ActionFilters actionFilters,
                                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(RemoveIndexLifecyclePolicyAction.NAME, transportService, clusterService, threadPool, actionFilters,
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
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        final Index[] indices = indexNameExpressionResolver.concreteIndices(state, request.indicesOptions(), request.indices());
        clusterService.submitStateUpdateTask("remove-lifecycle-for-index",
                new AckedClusterStateUpdateTask<Response>(request, listener) {

                    private final List<String> failedIndexes = new ArrayList<>();

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        return IndexLifecycleRunner.removePolicyForIndexes(indices, currentState, failedIndexes);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    protected Response newResponse(boolean acknowledged) {
                        return new Response(failedIndexes);
                    }
                });
    }

}
