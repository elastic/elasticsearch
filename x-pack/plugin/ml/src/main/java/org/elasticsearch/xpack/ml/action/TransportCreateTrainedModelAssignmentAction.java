/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction.Request;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction.Response;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentClusterService;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentNodeService;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentService;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;

public class TransportCreateTrainedModelAssignmentAction extends TransportMasterNodeAction<Request, Response> {

    private final TrainedModelAssignmentClusterService trainedModelAssignmentClusterService;

    @Inject
    public TransportCreateTrainedModelAssignmentAction(
        TrainedModelAssignmentClusterService trainedModelAssignmentClusterService,
        TrainedModelAssignmentService trainedModelAssignmentService,
        DeploymentManager deploymentManager,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        XPackLicenseState licenseState
    ) {
        super(
            CreateTrainedModelAssignmentAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.SAME
        );
        this.trainedModelAssignmentClusterService = trainedModelAssignmentClusterService;
        // Here we create our singleton for the node service
        clusterService.addListener(
            new TrainedModelAssignmentNodeService(
                trainedModelAssignmentService,
                clusterService,
                deploymentManager,
                indexNameExpressionResolver,
                transportService.getTaskManager(),
                threadPool,
                licenseState
            )
        );
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        trainedModelAssignmentClusterService.createNewModelAssignment(
            request.getTaskParams(),
            listener.delegateFailureAndWrap((l, trainedModelAssignment) -> l.onResponse(new Response(trainedModelAssignment)))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
