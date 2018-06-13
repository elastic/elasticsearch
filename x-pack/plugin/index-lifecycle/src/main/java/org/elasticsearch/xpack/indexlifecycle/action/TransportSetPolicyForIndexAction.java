/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.ResourceNotFoundException;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.action.SetPolicyForIndexAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.SetPolicyForIndexAction.Request;
import org.elasticsearch.xpack.core.indexlifecycle.action.SetPolicyForIndexAction.Response;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleRunner;

import java.util.ArrayList;
import java.util.List;

public class TransportSetPolicyForIndexAction extends TransportMasterNodeAction<Request, Response> {

    @Inject
    public TransportSetPolicyForIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
            ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, SetPolicyForIndexAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, Request::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected Response newResponse() {
        return new Response();
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        final String newPolicyName = request.policy();
        final Index[] indices = indexNameExpressionResolver.concreteIndices(state, request.indicesOptions(), request.indices());
        clusterService.submitStateUpdateTask("change-lifecycle-for-index-" + newPolicyName,
                new AckedClusterStateUpdateTask<Response>(request, listener) {

                    private final List<String> failedIndexes = new ArrayList<>();

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        IndexLifecycleMetadata ilmMetadata = (IndexLifecycleMetadata) currentState.metaData()
                                .custom(IndexLifecycleMetadata.TYPE);
                        LifecyclePolicy newPolicy = ilmMetadata.getPolicies().get(newPolicyName);

                        if (newPolicy == null) {
                            throw new ResourceNotFoundException("Policy does not exist [{}]", newPolicyName);
                        }

                        return IndexLifecycleRunner.setPolicyForIndexes(newPolicyName, indices, currentState, newPolicy, failedIndexes);
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
