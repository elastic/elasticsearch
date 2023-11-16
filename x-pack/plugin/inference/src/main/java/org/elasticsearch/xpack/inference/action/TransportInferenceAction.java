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
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.inference.UnparsedModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

public class TransportInferenceAction extends HandledTransportAction<InferenceAction.Request, InferenceAction.Response> {

    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;

    @Inject
    public TransportInferenceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry
    ) {
        super(InferenceAction.NAME, transportService, actionFilters, InferenceAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    protected void doExecute(Task task, InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {

        ActionListener<ModelRegistry.ModelConfigMap> getModelListener = ActionListener.wrap(modelConfigMap -> {
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

            if (request.getTaskType() != unparsedModel.taskType()) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Incompatible task_type, the requested type [{}] does not match the model type [{}]",
                        RestStatus.BAD_REQUEST,
                        request.getTaskType(),
                        unparsedModel.taskType()
                    )
                );
                return;
            }

            var model = service.get()
                .parsePersistedConfig(unparsedModel.modelId(), unparsedModel.taskType(), unparsedModel.settings(), unparsedModel.secrets());
            inferOnService(model, request, service.get(), listener);
        }, listener::onFailure);

        modelRegistry.getUnparsedModelMap(request.getModelId(), getModelListener);
    }

    private void inferOnService(
        Model model,
        InferenceAction.Request request,
        InferenceService service,
        ActionListener<InferenceAction.Response> listener
    ) {
        service.infer(model, request.getInput(), request.getTaskSettings(), ActionListener.wrap(inferenceResults -> {
            listener.onResponse(new InferenceAction.Response(inferenceResults));
        }, listener::onFailure));
    }
}
