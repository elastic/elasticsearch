/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

public class TransportGetInferenceModelAction extends HandledTransportAction<
    GetInferenceModelAction.Request,
    GetInferenceModelAction.Response> {

    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final Executor executor;

    @Inject
    public TransportGetInferenceModelAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
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
        this.executor = threadPool.executor(InferencePlugin.UTILITY_THREAD_POOL_NAME);
    }

    @Override
    protected void doExecute(
        Task task,
        GetInferenceModelAction.Request request,
        ActionListener<GetInferenceModelAction.Response> listener
    ) {
        boolean modelIdIsWildCard = Strings.isAllOrWildcard(request.getModelId());

        if (request.getTaskType() == TaskType.ANY && modelIdIsWildCard) {
            getAllModels(listener);
        } else if (modelIdIsWildCard) {
            getModelsByTaskType(request.getTaskType(), listener);
        } else {
            getSingleModel(request.getModelId(), request.getTaskType(), listener);
        }
    }

    private void getSingleModel(String modelId, TaskType requestedTaskType, ActionListener<GetInferenceModelAction.Response> listener) {
        modelRegistry.getModel(modelId, listener.delegateFailureAndWrap((delegate, unparsedModel) -> {
            var service = serviceRegistry.getService(unparsedModel.service());
            if (service.isEmpty()) {
                delegate.onFailure(
                    new ElasticsearchStatusException(
                        "Unknown service [{}] for model [{}]. ",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        unparsedModel.service(),
                        unparsedModel.modelId()
                    )
                );
                return;
            }

            if (requestedTaskType.isAnyOrSame(unparsedModel.taskType()) == false) {
                delegate.onFailure(
                    new ElasticsearchStatusException(
                        "Requested task type [{}] does not match the model's task type [{}]",
                        RestStatus.BAD_REQUEST,
                        requestedTaskType,
                        unparsedModel.taskType()
                    )
                );
                return;
            }

            var model = service.get().parsePersistedConfig(unparsedModel.modelId(), unparsedModel.taskType(), unparsedModel.settings());
            delegate.onResponse(new GetInferenceModelAction.Response(List.of(model.getConfigurations())));
        }));
    }

    private void getAllModels(ActionListener<GetInferenceModelAction.Response> listener) {
        modelRegistry.getAllModels(
            listener.delegateFailureAndWrap((l, models) -> executor.execute(ActionRunnable.supply(l, () -> parseModels(models))))
        );
    }

    private void getModelsByTaskType(TaskType taskType, ActionListener<GetInferenceModelAction.Response> listener) {
        modelRegistry.getModelsByTaskType(
            taskType,
            listener.delegateFailureAndWrap((l, models) -> executor.execute(ActionRunnable.supply(l, () -> parseModels(models))))
        );
    }

    private GetInferenceModelAction.Response parseModels(List<ModelRegistry.UnparsedModel> unparsedModels) {
        var parsedModels = new ArrayList<ModelConfigurations>();

        for (var unparsedModel : unparsedModels) {
            var service = serviceRegistry.getService(unparsedModel.service());
            if (service.isEmpty()) {
                throw new ElasticsearchStatusException(
                    "Unknown service [{}] for model [{}]. ",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    unparsedModel.service(),
                    unparsedModel.modelId()
                );
            }
            parsedModels.add(
                service.get()
                    .parsePersistedConfig(unparsedModel.modelId(), unparsedModel.taskType(), unparsedModel.settings())
                    .getConfigurations()
            );
        }
        return new GetInferenceModelAction.Response(parsedModels);
    }
}
