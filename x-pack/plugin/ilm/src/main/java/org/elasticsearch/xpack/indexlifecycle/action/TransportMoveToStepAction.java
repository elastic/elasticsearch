/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepAction.Request;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepAction.Response;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleService;

public class TransportMoveToStepAction extends TransportMasterNodeAction<Request, Response> {
    IndexLifecycleService indexLifecycleService;
    @Inject
    public TransportMoveToStepAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver,
                                     IndexLifecycleService indexLifecycleService) {
        super(settings, MoveToStepAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
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

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) {
        IndexMetaData indexMetaData = state.metaData().index(request.getIndex());
        if (indexMetaData == null) {
            listener.onFailure(new IllegalArgumentException("index [" + request.getIndex() + "] does not exist"));
            return;
        }
        clusterService.submitStateUpdateTask("index[" + request.getIndex() + "]-move-to-step",
            new AckedClusterStateUpdateTask<Response>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final ClusterState movedState = indexLifecycleService.moveClusterStateToStep(currentState, request.getIndex(),
                        request.getCurrentStepKey(), request.getNextStepKey());
                    final IndexMetaData indexMeta = movedState.metaData().index(request.getIndex());
                    final IndexMetaData newIndexMeta = IndexMetaData.builder(indexMeta)
                        .settings(Settings.builder()
                            .put(indexMeta.getSettings())
                            // Indicate that we have forced the index into this phase, therefore it
                            // should not have to wait until the phase's "after" time
                            .put(LifecycleSettings.LIFECYCLE_FORCED_PHASE, request.getNextStepKey().getPhase()))
                        .build();
                    final ClusterState finalState = ClusterState.builder(movedState)
                        .metaData(MetaData.builder(movedState.metaData())
                            .put(newIndexMeta, true))
                        .build();
                    return finalState;
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
