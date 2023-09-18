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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.UnparsedModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.registry.ServiceRegistry;
import org.elasticsearch.xpack.inference.services.InferenceService;

public class TransportInferenceAction extends HandledTransportAction<InferenceAction.Request, InferenceAction.Response> {

    private final ModelRegistry modelRegistry;
    private final ServiceRegistry serviceRegistry;

    @Inject
    public TransportInferenceAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ModelRegistry modelRegistry,
        ServiceRegistry serviceRegistry
    ) {
        super(InferenceAction.NAME, transportService, actionFilters, InferenceAction.Request::new);
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    protected void doExecute(Task task, InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {

        ActionListener<ModelRegistry.ModelConfigMap> getModelListener = ActionListener.wrap(modelConfigMap -> {
            var unparsedModel = UnparsedModel.unparsedModelFromMap(modelConfigMap.config());
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

            var model = service.get().parseConfigLenient(unparsedModel.modelId(), unparsedModel.taskType(), unparsedModel.settings());
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
        service.infer(model, request.getInput(), request.getTaskSettings(), ActionListener.wrap(inferenceResult -> {
            listener.onResponse(new InferenceAction.Response(inferenceResult));
        }, listener::onFailure));
    }
}
