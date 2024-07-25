/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentClusterService;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class TransportUpdateTrainedModelDeploymentAction extends TransportMasterNodeAction<
    UpdateTrainedModelDeploymentAction.Request,
    CreateTrainedModelAssignmentAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportUpdateTrainedModelDeploymentAction.class);

    private final TrainedModelAssignmentClusterService trainedModelAssignmentClusterService;
    private final InferenceAuditor auditor;

    @Inject
    public TransportUpdateTrainedModelDeploymentAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TrainedModelAssignmentClusterService trainedModelAssignmentClusterService,
        InferenceAuditor auditor
    ) {
        super(
            UpdateTrainedModelDeploymentAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateTrainedModelDeploymentAction.Request::new,
            indexNameExpressionResolver,
            CreateTrainedModelAssignmentAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.trainedModelAssignmentClusterService = Objects.requireNonNull(trainedModelAssignmentClusterService);
        this.auditor = Objects.requireNonNull(auditor);
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateTrainedModelDeploymentAction.Request request,
        ClusterState state,
        ActionListener<CreateTrainedModelAssignmentAction.Response> listener
    ) throws Exception {
        logger.debug(
            () -> format(
                "[%s] received request to update number of allocations to [%s]",
                request.getDeploymentId(),
                request.getNumberOfAllocations()
            )
        );

        trainedModelAssignmentClusterService.updateDeployment(
            request.getDeploymentId(),
            request.getNumberOfAllocations(),
            request.getAdaptiveAllocationsSettings(),
            request.isInternal(),
            ActionListener.wrap(updatedAssignment -> {
                auditor.info(
                    request.getDeploymentId(),
                    Messages.getMessage(Messages.INFERENCE_DEPLOYMENT_UPDATED_NUMBER_OF_ALLOCATIONS, request.getNumberOfAllocations())
                );
                listener.onResponse(new CreateTrainedModelAssignmentAction.Response(updatedAssignment));
            }, listener::onFailure)
        );
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateTrainedModelDeploymentAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
