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
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.ml.inference.loadingservice.Model;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;


public class TransportInferModelAction extends HandledTransportAction<InferModelAction.Request, InferModelAction.Response> {

    private final ModelLoadingService modelLoadingService;
    private final Client client;
    private final XPackLicenseState licenseState;

    @Inject
    public TransportInferModelAction(TransportService transportService,
                                     ActionFilters actionFilters,
                                     ModelLoadingService modelLoadingService,
                                     Client client,
                                     XPackLicenseState licenseState) {
        super(InferModelAction.NAME, transportService, actionFilters, InferModelAction.Request::new);
        this.modelLoadingService = modelLoadingService;
        this.client = client;
        this.licenseState = licenseState;
    }

    @Override
    protected void doExecute(Task task, InferModelAction.Request request, ActionListener<InferModelAction.Response> listener) {

        if (licenseState.isMachineLearningAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        ActionListener<Model> getModelListener = ActionListener.wrap(
            model -> {
                TypedChainTaskExecutor<InferenceResults> typedChainTaskExecutor =
                    new TypedChainTaskExecutor<>(client.threadPool().executor(ThreadPool.Names.SAME),
                    // run through all tasks
                    r -> true,
                    // Always fail immediately and return an error
                    ex -> true);
                request.getObjectsToInfer().forEach(stringObjectMap ->
                    typedChainTaskExecutor.add(chainedTask ->
                        model.infer(stringObjectMap, request.getConfig(), chainedTask)));

                typedChainTaskExecutor.execute(ActionListener.wrap(
                    inferenceResultsInterfaces ->
                        listener.onResponse(new InferModelAction.Response(inferenceResultsInterfaces)),
                    listener::onFailure
                ));
            },
            listener::onFailure
        );

        this.modelLoadingService.getModel(request.getModelId(), getModelListener);
    }
}
