/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.inference.UnparsedModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

public class TransportGetInferenceModelAction extends HandledTransportAction<
    GetInferenceModelAction.Request,
    PutInferenceModelAction.Response> {

    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;

    @Inject
    public TransportGetInferenceModelAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry
    ) {
        super(
            GetInferenceModelAction.NAME,
            transportService,
            actionFilters,
            GetInferenceModelAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    protected void doExecute(
        Task task,
        GetInferenceModelAction.Request request,
        ActionListener<PutInferenceModelAction.Response> listener
    ) {
        modelRegistry.getUnparsedModelMap(request.getModelId(), ActionListener.wrap(modelConfigMap -> {
            var unparsedModel = UnparsedModel.unparsedModelFromMap(modelConfigMap.config(), modelConfigMap.secrets());
            var service = serviceRegistry.getService(unparsedModel.service());
            if (service.isEmpty()) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Unknown service [{}] for model [{}]. ",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        unparsedModel.service(),
                        unparsedModel.modelId()
                    )
                );
                return;
            }
            var model = service.get()
                .parsePersistedConfig(unparsedModel.modelId(), unparsedModel.taskType(), unparsedModel.settings(), unparsedModel.secrets());
            listener.onResponse(new PutInferenceModelAction.Response(model.getConfigurations()));
        }, listener::onFailure));
    }
}
