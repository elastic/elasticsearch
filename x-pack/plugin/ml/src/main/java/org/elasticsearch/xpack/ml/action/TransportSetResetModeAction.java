/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.SetResetModeAction;


public class TransportSetResetModeAction extends AcknowledgedTransportMasterNodeAction<SetResetModeAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportSetResetModeAction.class);
    private final ClusterService clusterService;

    @Inject
    public TransportSetResetModeAction(TransportService transportService, ThreadPool threadPool, ClusterService clusterService,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(SetResetModeAction.NAME, transportService, clusterService, threadPool, actionFilters, SetResetModeAction.Request::new,
            indexNameExpressionResolver, ThreadPool.Names.SAME);
        this.clusterService = clusterService;
    }

    @Override
    protected void masterOperation(Task task,
                                   SetResetModeAction.Request request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {

        // Noop, nothing for us to do, simply return fast to the caller
        if (request.isEnabled() == MlMetadata.getMlMetadata(state).isResetMode()) {
            logger.debug("Reset mode noop");
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        logger.debug(
            () -> new ParameterizedMessage(
                "Starting to set [reset_mode] to [{}] from [{}]", request.isEnabled(), MlMetadata.getMlMetadata(state).isResetMode()
            )
        );

        ActionListener<AcknowledgedResponse> wrappedListener = ActionListener.wrap(
            r -> {
                logger.debug("Completed reset mode request");
                listener.onResponse(r);
            },
            e -> {
                logger.debug("Completed reset mode request but with failure", e);
                listener.onFailure(e);
            }
        );

        ActionListener<AcknowledgedResponse> clusterStateUpdateListener = ActionListener.wrap(
            acknowledgedResponse -> {
                if (acknowledgedResponse.isAcknowledged() == false) {
                    logger.info("Cluster state update is NOT acknowledged");
                    wrappedListener.onFailure(new ElasticsearchTimeoutException("Unknown error occurred while updating cluster state"));
                    return;
                }
                wrappedListener.onResponse(acknowledgedResponse);
            },
            wrappedListener::onFailure
        );

        clusterService.submitStateUpdateTask("ml-set-reset-mode",
            new AckedClusterStateUpdateTask(request, clusterStateUpdateListener) {

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    logger.trace(() -> new ParameterizedMessage("Cluster update response built: {}", acknowledged));
                    return AcknowledgedResponse.of(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    logger.trace("Executing cluster state update");
                    MlMetadata.Builder builder = MlMetadata.Builder
                        .from(currentState.metadata().custom(MlMetadata.TYPE))
                        .isResetMode(request.isEnabled());
                    ClusterState.Builder newState = ClusterState.builder(currentState);
                    newState.metadata(Metadata.builder(currentState.getMetadata()).putCustom(MlMetadata.TYPE, builder.build()).build());
                    return newState.build();
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(SetResetModeAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
