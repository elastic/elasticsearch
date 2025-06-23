/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.InferenceActionProxy;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;

public class TransportInferenceActionProxy extends HandledTransportAction<InferenceActionProxy.Request, InferenceAction.Response> {
    private final ModelRegistry modelRegistry;
    private final Client client;

    @Inject
    public TransportInferenceActionProxy(
        TransportService transportService,
        ActionFilters actionFilters,
        ModelRegistry modelRegistry,
        Client client
    ) {
        super(
            InferenceActionProxy.NAME,
            transportService,
            actionFilters,
            InferenceActionProxy.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.modelRegistry = modelRegistry;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, InferenceActionProxy.Request request, ActionListener<InferenceAction.Response> listener) {
        try {
            ActionListener<UnparsedModel> getModelListener = listener.delegateFailureAndWrap((l, unparsedModel) -> {
                if (unparsedModel.taskType() == TaskType.CHAT_COMPLETION) {
                    sendUnifiedCompletionRequest(request, l);
                } else {
                    sendInferenceActionRequest(request, l);
                }
            });

            if (request.getTaskType() == TaskType.ANY) {
                modelRegistry.getModelWithSecrets(request.getInferenceEntityId(), getModelListener);
            } else if (request.getTaskType() == TaskType.CHAT_COMPLETION) {
                sendUnifiedCompletionRequest(request, listener);
            } else {
                sendInferenceActionRequest(request, listener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void sendUnifiedCompletionRequest(InferenceActionProxy.Request request, ActionListener<InferenceAction.Response> listener) {
        // format any validation exceptions from the rest -> transport path as UnifiedChatCompletionException
        var unifiedErrorFormatListener = listener.delegateResponse((l, e) -> l.onFailure(UnifiedChatCompletionException.fromThrowable(e)));

        try {
            if (request.isStreaming() == false) {
                throw new ElasticsearchStatusException(
                    "The [chat_completion] task type only supports streaming, please try again with the _stream API",
                    RestStatus.BAD_REQUEST
                );
            }

            UnifiedCompletionAction.Request unifiedRequest;
            try (
                var parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, request.getContent(), request.getContentType())
            ) {
                unifiedRequest = UnifiedCompletionAction.Request.parseRequest(
                    request.getInferenceEntityId(),
                    request.getTaskType(),
                    request.getTimeout(),
                    request.getContext(),
                    parser
                );
            }

            execute(UnifiedCompletionAction.INSTANCE, unifiedRequest, listener);
        } catch (Exception e) {
            unifiedErrorFormatListener.onFailure(e);
        }
    }

    private void sendInferenceActionRequest(InferenceActionProxy.Request request, ActionListener<InferenceAction.Response> listener)
        throws IOException {
        InferenceAction.Request.Builder inferenceActionRequestBuilder;
        try (var parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, request.getContent(), request.getContentType())) {
            inferenceActionRequestBuilder = InferenceAction.Request.parseRequest(
                request.getInferenceEntityId(),
                request.getTaskType(),
                request.getContext(),
                parser
            );
            inferenceActionRequestBuilder.setInferenceTimeout(request.getTimeout()).setStream(request.isStreaming());
        }

        execute(InferenceAction.INSTANCE, inferenceActionRequestBuilder.build(), listener);
    }

    private <Request extends ActionRequest, Response extends ActionResponse> void execute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        var threadContext = client.threadPool().getThreadContext();
        // stash the context so we clear the user's security headers, then restore and copy the response headers
        var supplier = threadContext.newRestorableContext(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(INFERENCE_ORIGIN)) {
            client.execute(action, request, new ContextPreservingActionListener<>(supplier, listener));
        }
    }
}
