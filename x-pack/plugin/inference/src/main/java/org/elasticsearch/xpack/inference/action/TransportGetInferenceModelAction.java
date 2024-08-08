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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.common.InferenceExceptions;
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
        boolean inferenceEntityIdIsWildCard = Strings.isAllOrWildcard(request.getInferenceEntityId());

        if (request.getTaskType() == TaskType.ANY && inferenceEntityIdIsWildCard) {
            getAllModels(listener);
        } else if (inferenceEntityIdIsWildCard) {
            getModelsByTaskType(request.getTaskType(), listener);
        } else {
            getSingleModel(request.getInferenceEntityId(), request.getTaskType(), listener);
        }
    }

    private void getSingleModel(
        String inferenceEntityId,
        TaskType requestedTaskType,
        ActionListener<GetInferenceModelAction.Response> listener
    ) {
        modelRegistry.getModel(inferenceEntityId, listener.delegateFailureAndWrap((delegate, unparsedModel) -> {
            var service = serviceRegistry.getService(unparsedModel.service());
            if (service.isEmpty()) {
                delegate.onFailure(serviceNotFoundException(unparsedModel.service(), unparsedModel.inferenceEntityId()));
                return;
            }

            if (requestedTaskType.isAnyOrSame(unparsedModel.taskType()) == false) {
                delegate.onFailure(InferenceExceptions.mismatchedTaskTypeException(requestedTaskType, unparsedModel.taskType()));
                return;
            }

            var model = service.get()
                .parsePersistedConfig(unparsedModel.inferenceEntityId(), unparsedModel.taskType(), unparsedModel.settings());
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
                throw serviceNotFoundException(unparsedModel.service(), unparsedModel.inferenceEntityId());
            }
            parsedModels.add(
                service.get()
                    .parsePersistedConfig(unparsedModel.inferenceEntityId(), unparsedModel.taskType(), unparsedModel.settings())
                    .getConfigurations()
            );
        }
        return new GetInferenceModelAction.Response(parsedModels);
    }

    private ElasticsearchStatusException serviceNotFoundException(String service, String inferenceId) {
        throw new ElasticsearchStatusException(
            "Unknown service [{}] for inference endpoint [{}]. ",
            RestStatus.INTERNAL_SERVER_ERROR,
            service,
            inferenceId
        );
    }
}
