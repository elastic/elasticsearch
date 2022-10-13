/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction.Request;
import org.elasticsearch.xpack.core.ml.action.InferModelAction.Response;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportInternalInferModelAction extends HandledTransportAction<Request, Response> {

    private final ModelLoadingService modelLoadingService;
    private final Client client;
    private final ClusterService clusterService;
    private final XPackLicenseState licenseState;
    private final TrainedModelProvider trainedModelProvider;

    TransportInternalInferModelAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ModelLoadingService modelLoadingService,
        Client client,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        TrainedModelProvider trainedModelProvider
    ) {
        super(actionName, transportService, actionFilters, InferModelAction.Request::new);
        this.modelLoadingService = modelLoadingService;
        this.client = client;
        this.clusterService = clusterService;
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
    }

    @Inject
    public TransportInternalInferModelAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ModelLoadingService modelLoadingService,
        Client client,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        TrainedModelProvider trainedModelProvider
    ) {
        this(
            InferModelAction.NAME,
            transportService,
            actionFilters,
            modelLoadingService,
            client,
            clusterService,
            licenseState,
            trainedModelProvider
        );
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {

        Response.Builder responseBuilder = Response.builder();
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());

        if (MachineLearning.INFERENCE_AGG_FEATURE.check(licenseState)) {
            responseBuilder.setLicensed(true);
            doInfer(task, request, responseBuilder, parentTaskId, listener);
        } else {
            trainedModelProvider.getTrainedModel(
                request.getModelId(),
                GetTrainedModelsAction.Includes.empty(),
                parentTaskId,
                ActionListener.wrap(trainedModelConfig -> {
                    // Since we just checked MachineLearningField.ML_API_FEATURE.check(licenseState) and that check failed
                    // That means we don't have a plat+ license. The only licenses for trained models are basic (free) and plat.
                    boolean allowed = trainedModelConfig.getLicenseLevel() == License.OperationMode.BASIC;
                    responseBuilder.setLicensed(allowed);
                    if (allowed || request.isPreviouslyLicensed()) {
                        doInfer(task, request, responseBuilder, parentTaskId, listener);
                    } else {
                        listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
                    }
                }, listener::onFailure)
            );
        }
    }

    private void doInfer(
        Task task,
        Request request,
        Response.Builder responseBuilder,
        TaskId parentTaskId,
        ActionListener<Response> listener
    ) {
        if (isAllocatedModel(request.getModelId())) {
            inferAgainstAllocatedModel(request, responseBuilder, parentTaskId, listener);
        } else {
            getModelAndInfer(request, responseBuilder, parentTaskId, (CancellableTask) task, listener);
        }
    }

    private boolean isAllocatedModel(String modelId) {
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = TrainedModelAssignmentMetadata.fromState(clusterService.state());
        return trainedModelAssignmentMetadata.isAssigned(modelId);
    }

    private void getModelAndInfer(
        Request request,
        Response.Builder responseBuilder,
        TaskId parentTaskId,
        CancellableTask task,
        ActionListener<Response> listener
    ) {
        ActionListener<LocalModel> getModelListener = ActionListener.wrap(model -> {
            TypedChainTaskExecutor<InferenceResults> typedChainTaskExecutor = new TypedChainTaskExecutor<>(
                client.threadPool().executor(ThreadPool.Names.SAME),
                // run through all tasks
                r -> true,
                // Always fail immediately and return an error
                ex -> true
            );
            request.getObjectsToInfer().forEach(stringObjectMap -> typedChainTaskExecutor.add(chainedTask -> {
                if (task.isCancelled()) {
                    throw new TaskCancelledException(format("Inference task cancelled with reason [%s]", task.getReasonCancelled()));
                }
                model.infer(stringObjectMap, request.getUpdate(), chainedTask);
            }));

            typedChainTaskExecutor.execute(ActionListener.wrap(inferenceResultsInterfaces -> {
                model.release();
                listener.onResponse(responseBuilder.setInferenceResults(inferenceResultsInterfaces).setModelId(model.getModelId()).build());
            }, e -> {
                model.release();
                listener.onFailure(e);
            }));
        }, listener::onFailure);

        modelLoadingService.getModelForPipeline(request.getModelId(), parentTaskId, getModelListener);
    }

    private void inferAgainstAllocatedModel(
        Request request,
        Response.Builder responseBuilder,
        TaskId parentTaskId,
        ActionListener<Response> listener
    ) {
        TypedChainTaskExecutor<InferenceResults> typedChainTaskExecutor = new TypedChainTaskExecutor<>(
            client.threadPool().executor(ThreadPool.Names.SAME),
            // run through all tasks
            r -> true,
            // Always fail immediately and return an error
            ex -> true
        );
        request.getObjectsToInfer()
            .forEach(
                stringObjectMap -> typedChainTaskExecutor.add(
                    chainedTask -> inferSingleDocAgainstAllocatedModel(
                        request.getModelId(),
                        request.getTimeout(),
                        request.getUpdate(),
                        stringObjectMap,
                        parentTaskId,
                        chainedTask
                    )
                )
            );

        typedChainTaskExecutor.execute(
            ActionListener.wrap(
                inferenceResults -> listener.onResponse(
                    responseBuilder.setInferenceResults(inferenceResults).setModelId(request.getModelId()).build()
                ),
                listener::onFailure
            )
        );
    }

    private void inferSingleDocAgainstAllocatedModel(
        String modelId,
        TimeValue timeValue,
        InferenceConfigUpdate inferenceConfigUpdate,
        Map<String, Object> doc,
        TaskId parentTaskId,
        ActionListener<InferenceResults> listener
    ) {
        InferTrainedModelDeploymentAction.Request request = new InferTrainedModelDeploymentAction.Request(
            modelId,
            inferenceConfigUpdate,
            Collections.singletonList(doc),
            timeValue
        );
        request.setParentTask(parentTaskId);
        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            InferTrainedModelDeploymentAction.INSTANCE,
            request,
            ActionListener.wrap(r -> listener.onResponse(r.getResults()), listener::onFailure)
        );
    }
}
