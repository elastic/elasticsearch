/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction.Request;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction.Response;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;


public class TransportInternalInferModelAction extends HandledTransportAction<Request, Response> {

    private final ModelLoadingService modelLoadingService;
    private final Client client;
    private final XPackLicenseState licenseState;
    private final TrainedModelProvider trainedModelProvider;

    @Inject
    public TransportInternalInferModelAction(TransportService transportService,
                                             ActionFilters actionFilters,
                                             ModelLoadingService modelLoadingService,
                                             Client client,
                                             XPackLicenseState licenseState,
                                             TrainedModelProvider trainedModelProvider) {
        super(InternalInferModelAction.NAME, transportService, actionFilters, InternalInferModelAction.Request::new);
        this.modelLoadingService = modelLoadingService;
        this.client = client;
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {

        Response.Builder responseBuilder = Response.builder();

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
                        listener.onResponse(responseBuilder.setInferenceResults(inferenceResultsInterfaces).build());
                    },
                    e -> {
                        model.release();
                        listener.onFailure(e);
                    }
                ));
            },
            listener::onFailure
        );

        if (licenseState.checkFeature(XPackLicenseState.Feature.MACHINE_LEARNING)) {
            responseBuilder.setLicensed(true);
            this.modelLoadingService.getModelForPipeline(request.getModelId(), getModelListener);
        } else {
            trainedModelProvider.getTrainedModel(request.getModelId(), false, ActionListener.wrap(
                trainedModelConfig -> {
                    responseBuilder.setLicensed(licenseState.isAllowedByLicense(trainedModelConfig.getLicenseLevel()));
                    if (licenseState.isAllowedByLicense(trainedModelConfig.getLicenseLevel()) || request.isPreviouslyLicensed()) {
                        this.modelLoadingService.getModelForPipeline(request.getModelId(), getModelListener);
                    } else {
                        listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
                    }
                },
                listener::onFailure
            ));
        }
    }
}
