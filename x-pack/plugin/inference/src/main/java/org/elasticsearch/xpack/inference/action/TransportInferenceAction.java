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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.telemetry.InferenceStats;

public class TransportInferenceAction extends HandledTransportAction<InferenceAction.Request, InferenceAction.Response> {
    private static final String STREAMING_INFERENCE_TASK_TYPE = "streaming_inference";
    private static final String STREAMING_TASK_ACTION = "xpack/inference/streaming_inference[n]";

    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final InferenceStats inferenceStats;
    private final StreamingTaskManager streamingTaskManager;

    @Inject
    public TransportInferenceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager
    ) {
        super(InferenceAction.NAME, transportService, actionFilters, InferenceAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.inferenceStats = inferenceStats;
        this.streamingTaskManager = streamingTaskManager;
    }

    @Override
    protected void doExecute(Task task, InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {

        ActionListener<ModelRegistry.UnparsedModel> getModelListener = listener.delegateFailureAndWrap((delegate, unparsedModel) -> {
            var service = serviceRegistry.getService(unparsedModel.service());
            if (service.isEmpty()) {
                delegate.onFailure(
                    new ElasticsearchStatusException(
                        "Unknown service [{}] for model [{}]. ",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        unparsedModel.service(),
                        unparsedModel.inferenceEntityId()
                    )
                );
                return;
            }

            if (request.getTaskType().isAnyOrSame(unparsedModel.taskType()) == false) {
                // not the wildcard task type and not the model task type
                delegate.onFailure(
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
                .parsePersistedConfigWithSecrets(
                    unparsedModel.inferenceEntityId(),
                    unparsedModel.taskType(),
                    unparsedModel.settings(),
                    unparsedModel.secrets()
                );
            inferenceStats.incrementRequestCount(model);
            inferOnService(model, request, service.get(), delegate);
        });

        modelRegistry.getModelWithSecrets(request.getInferenceEntityId(), getModelListener);
    }

    private void inferOnService(
        Model model,
        InferenceAction.Request request,
        InferenceService service,
        ActionListener<InferenceAction.Response> listener
    ) {
        service.infer(
            model,
            request.getQuery(),
            request.getInput(),
            request.getTaskSettings(),
            request.getInputType(),
            request.getInferenceTimeout(),
            createListener(request, listener)
        );
    }

    private ActionListener<InferenceServiceResults> createListener(
        InferenceAction.Request request,
        ActionListener<InferenceAction.Response> listener
    ) {
        if (request.isStreaming()) {
            return listener.delegateFailureAndWrap((l, inferenceResults) -> {
                if (inferenceResults.isStreaming()) {
                    var taskProcessor = streamingTaskManager.<ChunkedToXContent>create(
                        STREAMING_INFERENCE_TASK_TYPE,
                        STREAMING_TASK_ACTION
                    );
                    inferenceResults.publisher().subscribe(taskProcessor);
                    l.onResponse(new InferenceAction.Response(inferenceResults, taskProcessor));
                } else {
                    // if we asked for streaming but the provider doesn't support it, for now we're going to get back the single response
                    l.onResponse(new InferenceAction.Response(inferenceResults));
                }
            });
        }
        return listener.delegateFailureAndWrap((l, inferenceResults) -> l.onResponse(new InferenceAction.Response(inferenceResults)));
    };
}
