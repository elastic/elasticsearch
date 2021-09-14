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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction.Request;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction.Response;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationMetadata;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;


public class TransportInternalInferModelAction extends HandledTransportAction<Request, Response> {

    private final ModelLoadingService modelLoadingService;
    private final Client client;
    private final ClusterService clusterService;
    private final XPackLicenseState licenseState;
    private final TrainedModelProvider trainedModelProvider;

    @Inject
    public TransportInternalInferModelAction(TransportService transportService,
                                             ActionFilters actionFilters,
                                             ModelLoadingService modelLoadingService,
                                             Client client,
                                             ClusterService clusterService,
                                             XPackLicenseState licenseState,
                                             TrainedModelProvider trainedModelProvider) {
        super(InternalInferModelAction.NAME, transportService, actionFilters, InternalInferModelAction.Request::new);
        this.modelLoadingService = modelLoadingService;
        this.client = client;
        this.clusterService = clusterService;
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {

        Response.Builder responseBuilder = Response.builder();

        if (licenseState.checkFeature(XPackLicenseState.Feature.MACHINE_LEARNING)) {
            responseBuilder.setLicensed(true);
            doInfer(request, responseBuilder, listener);
        } else {
            trainedModelProvider.getTrainedModel(request.getModelId(), GetTrainedModelsAction.Includes.empty(), ActionListener.wrap(
                trainedModelConfig -> {
                    responseBuilder.setLicensed(licenseState.isAllowedByLicense(trainedModelConfig.getLicenseLevel()));
                    if (licenseState.isAllowedByLicense(trainedModelConfig.getLicenseLevel()) || request.isPreviouslyLicensed()) {
                        doInfer(request, responseBuilder, listener);
                    } else {
                        listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
                    }
                },
                listener::onFailure
            ));
        }
    }

    private void doInfer(Request request, Response.Builder responseBuilder, ActionListener<Response> listener) {
        if (isAllocatedModel(request.getModelId())) {
            inferAgainstAllocatedModel(request, responseBuilder, listener);
        } else {
            getModelAndInfer(request, responseBuilder, listener);
        }
    }

    private boolean isAllocatedModel(String modelId) {
        TrainedModelAllocationMetadata trainedModelAllocationMetadata = TrainedModelAllocationMetadata.fromState(clusterService.state());
        return trainedModelAllocationMetadata.isAllocated(modelId);
    }

    private void getModelAndInfer(Request request, Response.Builder responseBuilder, ActionListener<Response> listener) {
        ActionListener<LocalModel> getModelListener = ActionListener.wrap(
            model -> {
                TypedChainTaskExecutor<InferenceResults> typedChainTaskExecutor =
                    new TypedChainTaskExecutor<>(client.threadPool().executor(ThreadPool.Names.SAME),
                        // run through all tasks
                        r -> true,
                        // Always fail immediately and return an error
                        ex -> true);
                request.getObjectsToInfer().forEach(stringObjectMap ->
                    typedChainTaskExecutor.add(chainedTask ->
                        model.infer(stringObjectMap, request.getUpdate(), chainedTask)));

                typedChainTaskExecutor.execute(ActionListener.wrap(
                    inferenceResultsInterfaces -> {
                        model.release();
                        listener.onResponse(responseBuilder.setInferenceResults(inferenceResultsInterfaces)
                            .setModelId(model.getModelId())
                            .build());
                    },
                    e -> {
                        model.release();
                        listener.onFailure(e);
                    }
                ));
            },
            listener::onFailure
        );

        modelLoadingService.getModelForPipeline(request.getModelId(), getModelListener);
    }

    private void inferAgainstAllocatedModel(Request request, Response.Builder responseBuilder, ActionListener<Response> listener) {
        TypedChainTaskExecutor<InferenceResults> typedChainTaskExecutor =
            new TypedChainTaskExecutor<>(client.threadPool().executor(ThreadPool.Names.SAME),
                // run through all tasks
                r -> true,
                // Always fail immediately and return an error
                ex -> true);
        request.getObjectsToInfer().forEach(stringObjectMap -> typedChainTaskExecutor.add(
            chainedTask -> inferSingleDocAgainstAllocatedModel(request.getModelId(), stringObjectMap, chainedTask)));

        typedChainTaskExecutor.execute(ActionListener.wrap(
            inferenceResults -> listener.onResponse(responseBuilder.setInferenceResults(inferenceResults)
                    .setModelId(request.getModelId())
                    .build()),
            listener::onFailure
        ));
    }

    private void inferSingleDocAgainstAllocatedModel(String modelId, Map<String, Object> doc, ActionListener<InferenceResults> listener) {
        executeAsyncWithOrigin(client,
            ML_ORIGIN,
            InferTrainedModelDeploymentAction.INSTANCE,
            new InferTrainedModelDeploymentAction.Request(modelId, Collections.singletonList(doc)),
            ActionListener.wrap(
                r -> listener.onResponse(r.getResults()),
                e -> listener.onResponse(new WarningInferenceResults(e.getMessage()))
            )
        );
    }
}
