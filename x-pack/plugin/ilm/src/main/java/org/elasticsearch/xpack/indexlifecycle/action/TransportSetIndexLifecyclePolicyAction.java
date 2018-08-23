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
import org.elasticsearch.xpack.core.indexlifecycle.SetIndexLifecyclePolicyRequest;
import org.elasticsearch.xpack.core.indexlifecycle.SetIndexLifecyclePolicyResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.action.SetIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleRunner;

import java.util.ArrayList;
import java.util.List;

public class TransportSetIndexLifecyclePolicyAction
    extends TransportMasterNodeAction<SetIndexLifecyclePolicyRequest, SetIndexLifecyclePolicyResponse> {

    @Inject
    public TransportSetIndexLifecyclePolicyAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                                  ThreadPool threadPool, ActionFilters actionFilters,
                                                  IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, SetIndexLifecyclePolicyAction.NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, SetIndexLifecyclePolicyRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected SetIndexLifecyclePolicyResponse newResponse() {
        return new SetIndexLifecyclePolicyResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(SetIndexLifecyclePolicyRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(SetIndexLifecyclePolicyRequest request, ClusterState state,
                                   ActionListener<SetIndexLifecyclePolicyResponse> listener) throws Exception {
        final String newPolicyName = request.policy();
        final Index[] indices = indexNameExpressionResolver.concreteIndices(state, request.indicesOptions(), request.indices());
        clusterService.submitStateUpdateTask("change-lifecycle-for-index-" + newPolicyName,
                new AckedClusterStateUpdateTask<SetIndexLifecyclePolicyResponse>(request, listener) {

                    private final List<String> failedIndexes = new ArrayList<>();

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        IndexLifecycleMetadata ilmMetadata = (IndexLifecycleMetadata) currentState.metaData()
                                .custom(IndexLifecycleMetadata.TYPE);

                        if (ilmMetadata == null) {
                            throw new ResourceNotFoundException("Policy does not exist [{}]", newPolicyName);
                        }

                        LifecyclePolicy newPolicy = ilmMetadata.getPolicies().get(newPolicyName);

                        if (newPolicy == null) {
                            throw new ResourceNotFoundException("Policy does not exist [{}]", newPolicyName);
                        }

                        return IndexLifecycleRunner.setPolicyForIndexes(newPolicyName, indices, currentState, newPolicy, failedIndexes,
                                () -> System.currentTimeMillis());
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    protected SetIndexLifecyclePolicyResponse newResponse(boolean acknowledged) {
                        return new SetIndexLifecyclePolicyResponse(failedIndexes);
                    }
                });
    }

}
