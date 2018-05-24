/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.indexlifecycle.action.ReRunAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.ReRunAction.Request;
import org.elasticsearch.xpack.core.indexlifecycle.action.ReRunAction.Response;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleRunner;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleService;

public class TransportReRunAction extends TransportMasterNodeAction<Request, Response> {
    IndexLifecycleService indexLifecycleService;
    @Inject
    public TransportReRunAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                ThreadPool threadPool, ActionFilters actionFilters,
                                IndexNameExpressionResolver indexNameExpressionResolver,
                                IndexLifecycleService indexLifecycleService) {
        super(settings, ReRunAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                Request::new);
        this.indexLifecycleService = indexLifecycleService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected Response newResponse() {
        return new Response();
    }

    ClusterState innerExecute(ClusterState currentState, String[] indices) {
        ClusterState newState = currentState;
        for (String index : indices) {
            IndexMetaData indexMetaData = currentState.metaData().index(index);
            if (indexMetaData == null) {
                throw new IllegalArgumentException("index [" + index + "] does not exist");
            }
            StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(indexMetaData.getSettings());
            String failedStep = LifecycleSettings.LIFECYCLE_FAILED_STEP_SETTING.get(indexMetaData.getSettings());
            if (currentStepKey != null && ErrorStep.NAME.equals(currentStepKey.getName())
                && Strings.isNullOrEmpty(failedStep) == false) {
                StepKey nextStepKey = new StepKey(currentStepKey.getPhase(), currentStepKey.getAction(), failedStep);
                newState = indexLifecycleService.moveClusterStateToStep(currentState, index, currentStepKey, nextStepKey);
            } else {
                throw new IllegalArgumentException("cannot re-run an action for an index ["
                    + index + "] that has not encountered an error when running a Lifecycle Policy");
            }
        }
        return newState;
    }

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) {
        clusterService.submitStateUpdateTask("ilm-re-run",
            new AckedClusterStateUpdateTask<Response>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return innerExecute(currentState, request.indices());
                }

                @Override
                protected Response newResponse(boolean acknowledged) {
                    return new Response(acknowledged);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
