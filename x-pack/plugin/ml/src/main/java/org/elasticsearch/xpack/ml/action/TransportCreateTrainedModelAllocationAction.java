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
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAllocationAction;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAllocationAction.Request;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAllocationAction.Response;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationClusterService;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationNodeService;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationService;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;

public class TransportCreateTrainedModelAllocationAction extends TransportMasterNodeAction<Request, Response> {

    private final TrainedModelAllocationClusterService trainedModelAllocationClusterService;

    @Inject
    public TransportCreateTrainedModelAllocationAction(
        TrainedModelAllocationClusterService trainedModelAllocationClusterService,
        TrainedModelAllocationService trainedModelAllocationService,
        DeploymentManager deploymentManager,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        XPackLicenseState licenseState
    ) {
        super(
            CreateTrainedModelAllocationAction.NAME,
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
        this.trainedModelAllocationClusterService = trainedModelAllocationClusterService;
        // Here we create our singleton for the node service
        clusterService.addListener(
            new TrainedModelAllocationNodeService(
                trainedModelAllocationService,
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
        trainedModelAllocationClusterService.createNewModelAllocation(
            request.getTaskParams(),
            ActionListener.wrap(trainedModelAllocation -> listener.onResponse(new Response(trainedModelAllocation)), listener::onFailure)
        );
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
