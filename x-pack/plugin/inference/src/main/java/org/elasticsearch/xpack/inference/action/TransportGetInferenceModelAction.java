/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

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
            getAllModels(request.isPersistDefaultConfig(), listener);
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

            service.get()
                .updateModelsWithDynamicFields(
                    List.of(model),
                    delegate.delegateFailureAndWrap(
                        (l2, updatedModels) -> l2.onResponse(
                            new GetInferenceModelAction.Response(
                                updatedModels.stream().map(Model::getConfigurations).collect(Collectors.toList())
                            )
                        )
                    )
                );
        }));
    }

    private void getAllModels(boolean persistDefaultEndpoints, ActionListener<GetInferenceModelAction.Response> listener) {
        modelRegistry.getAllModels(
            persistDefaultEndpoints,
            listener.delegateFailureAndWrap((l, models) -> executor.execute(() -> parseModels(models, listener)))
        );
    }

    private void getModelsByTaskType(TaskType taskType, ActionListener<GetInferenceModelAction.Response> listener) {
        modelRegistry.getModelsByTaskType(
            taskType,
            listener.delegateFailureAndWrap((l, models) -> executor.execute(() -> parseModels(models, listener)))
        );
    }

    private void parseModels(List<UnparsedModel> unparsedModels, ActionListener<GetInferenceModelAction.Response> listener) {
        if (unparsedModels.isEmpty()) {
            listener.onResponse(new GetInferenceModelAction.Response(List.of()));
            return;
        }

        var parsedModelsByService = new HashMap<String, List<Model>>();
        try {
            for (var unparsedModel : unparsedModels) {
                var service = serviceRegistry.getService(unparsedModel.service());
                if (service.isEmpty()) {
                    throw serviceNotFoundException(unparsedModel.service(), unparsedModel.inferenceEntityId());
                }
                var list = parsedModelsByService.computeIfAbsent(service.get().name(), s -> new ArrayList<>());
                list.add(
                    service.get()
                        .parsePersistedConfig(unparsedModel.inferenceEntityId(), unparsedModel.taskType(), unparsedModel.settings())
                );
            }

            var groupedListener = new GroupedActionListener<List<Model>>(
                parsedModelsByService.entrySet().size(),
                listener.delegateFailureAndWrap((delegate, listOfListOfModels) -> {
                    var modifiable = new ArrayList<Model>();
                    for (var l : listOfListOfModels) {
                        modifiable.addAll(l);
                    }
                    modifiable.sort(Comparator.comparing(Model::getInferenceEntityId));
                    delegate.onResponse(
                        new GetInferenceModelAction.Response(modifiable.stream().map(Model::getConfigurations).collect(Collectors.toList()))
                    );
                })
            );

            for (var entry : parsedModelsByService.entrySet()) {
                serviceRegistry.getService(entry.getKey())
                    .get()  // must be non-null to get this far
                    .updateModelsWithDynamicFields(entry.getValue(), groupedListener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private ElasticsearchStatusException serviceNotFoundException(String service, String inferenceId) {
        throw new ElasticsearchStatusException(
            "Unknown service [{}] for inference endpoint [{}].",
            RestStatus.INTERNAL_SERVER_ERROR,
            service,
            inferenceId
        );
    }
}
