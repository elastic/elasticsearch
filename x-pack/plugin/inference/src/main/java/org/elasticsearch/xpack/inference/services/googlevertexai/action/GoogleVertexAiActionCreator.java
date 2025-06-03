/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiRerankRequestManager;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiCompletionModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class GoogleVertexAiActionCreator implements GoogleVertexAiActionVisitor {

    public static final String COMPLETION_ERROR_PREFIX = "Google VertexAI chat completion";
    private final Sender sender;

    private final ServiceComponents serviceComponents;

    static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new GoogleVertexAiUnifiedChatCompletionResponseHandler(
        "Google VertexAI chat completion"
    );
    static final ResponseHandler CHAT_COMPLETION_HANDLER = new GoogleVertexAiChatCompletionResponseHandler("Google VertexAI completion");
    static final String USER_ROLE = "user";

    public GoogleVertexAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(GoogleVertexAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = GoogleVertexAiEmbeddingsModel.of(model, taskSettings);
        var requestManager = new GoogleVertexAiEmbeddingsRequestManager(
            overriddenModel,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("Google Vertex AI embeddings");
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(GoogleVertexAiRerankModel model, Map<String, Object> taskSettings) {
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("Google Vertex AI rerank");
        var requestManager = GoogleVertexAiRerankRequestManager.of(model, serviceComponents.threadPool());
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    @Override
    public ExecutableAction create(GoogleVertexAiChatCompletionModel model, Map<String, Object> taskSettings) {

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(COMPLETION_ERROR_PREFIX);
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            UNIFIED_CHAT_COMPLETION_HANDLER,
            inputs -> new GoogleVertexAiUnifiedChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), model),
            ChatCompletionInput.class
        );

        return new SingleInputSenderExecutableAction(sender, manager, failedToSendRequestErrorMessage, COMPLETION_ERROR_PREFIX);
    }

    @Override
    public ExecutableAction create(GoogleVertexAiCompletionModel model, Map<String, Object> taskSettings) {
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(COMPLETION_ERROR_PREFIX);

        var manager = new GenericRequestManager<>(serviceComponents.threadPool(), model, CHAT_COMPLETION_HANDLER, inputs -> {
            try {
                model.updateUri(inputs.stream());
            } catch (URISyntaxException e) {
                throw new ElasticsearchStatusException(
                    "Error constructing URI for Google VertexAI completion",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    e
                );
            }
            return new GoogleVertexAiUnifiedChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), model);
        }, ChatCompletionInput.class);

        return new SingleInputSenderExecutableAction(sender, manager, failedToSendRequestErrorMessage, COMPLETION_ERROR_PREFIX);
    }
}
