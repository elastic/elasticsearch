/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentClusterService;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportUpdateTrainedModelDeploymentAction extends TransportMasterNodeAction<
    UpdateTrainedModelDeploymentAction.Request,
    CreateTrainedModelAssignmentAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportUpdateTrainedModelDeploymentAction.class);

    private final TrainedModelAssignmentClusterService trainedModelAssignmentClusterService;
    private final InferenceAuditor auditor;
    private final ProjectResolver projectResolver;
    private final Client client;

    @Inject
    public TransportUpdateTrainedModelDeploymentAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        TrainedModelAssignmentClusterService trainedModelAssignmentClusterService,
        InferenceAuditor auditor,
        ProjectResolver projectResolver,
        Client client
    ) {
        super(
            UpdateTrainedModelDeploymentAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateTrainedModelDeploymentAction.Request::new,
            CreateTrainedModelAssignmentAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.trainedModelAssignmentClusterService = Objects.requireNonNull(trainedModelAssignmentClusterService);
        this.auditor = Objects.requireNonNull(auditor);
        this.projectResolver = Objects.requireNonNull(projectResolver);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ML_ORIGIN);
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateTrainedModelDeploymentAction.Request request,
        ClusterState state,
        ActionListener<CreateTrainedModelAssignmentAction.Response> listener
    ) throws Exception {
        logger.debug(
            "[{}] received request to update number of allocations to [{}]",
            request.getDeploymentId(),
            request.getNumberOfAllocations()
        );

        checkIfUsedByDefaultInferenceEndpoint(task, request, listener.delegateFailureAndWrap((l, unused) -> updateDeployment(request, l)));
    }

    private void checkIfUsedByDefaultInferenceEndpoint(
        Task task,
        UpdateTrainedModelDeploymentAction.Request request,
        ActionListener<Void> listener
    ) {
        if (request.isInternal()) {
            listener.onResponse(null);
            return;
        }

        var deploymentId = request.getDeploymentId();
        var parentClient = new ParentTaskAssigningClient(client, clusterService.localNode(), task);
        var getAllEndpoints = new GetInferenceModelAction.Request("*", TaskType.ANY);
        if (request.ackTimeout() != null) {
            getAllEndpoints.ackTimeout(request.ackTimeout());
        }

        // if this deployment was created by an inference endpoint, then it must be updated by the inference endpoint _update API
        parentClient.execute(GetInferenceModelAction.INSTANCE, getAllEndpoints, listener.delegateFailureAndWrap((l, response) -> {
            response.getEndpoints()
                .stream()
                .filter(model -> model.getService().equals("elasticsearch") || model.getService().equals("elser"))
                .map(ModelConfigurations::getInferenceEntityId)
                .filter(deploymentId::equals)
                .findAny()
                .ifPresentOrElse(
                    endpointId -> l.onFailure(
                        new ElasticsearchStatusException(
                            "Cannot update deployment [{}] as it was created by inference endpoint [{}]. "
                                + "This model deployment must be updated through the inference API.",
                            RestStatus.CONFLICT,
                            deploymentId,
                            endpointId
                        )
                    ),
                    () -> l.onResponse(null)
                );
        }));
    }

    private void updateDeployment(
        UpdateTrainedModelDeploymentAction.Request request,
        ActionListener<CreateTrainedModelAssignmentAction.Response> listener
    ) {
        trainedModelAssignmentClusterService.updateDeployment(
            request.getDeploymentId(),
            request.getNumberOfAllocations(),
            request.getAdaptiveAllocationsSettings(),
            request.isInternal(),
            listener.delegateFailureAndWrap((l, updatedAssignment) -> {
                auditor.info(
                    request.getDeploymentId(),
                    Messages.getMessage(Messages.INFERENCE_DEPLOYMENT_UPDATED_NUMBER_OF_ALLOCATIONS, request.getNumberOfAllocations())
                );
                listener.onResponse(new CreateTrainedModelAssignmentAction.Response(updatedAssignment));
            })
        );
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateTrainedModelDeploymentAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
