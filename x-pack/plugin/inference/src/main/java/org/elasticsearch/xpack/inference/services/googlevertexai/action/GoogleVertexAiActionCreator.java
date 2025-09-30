/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicResponseHandler;
import org.elasticsearch.xpack.inference.services.anthropic.response.AnthropicChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiRerankRequestManager;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiResponseHandler;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.completion.GoogleVertexAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.response.GoogleVertexAiCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class GoogleVertexAiActionCreator implements GoogleVertexAiActionVisitor {

    public static final String COMPLETION_ERROR_PREFIX = "Google VertexAI chat completion";
    private final Sender sender;

    private final ServiceComponents serviceComponents;

    static final ResponseHandler GOOGLE_VERTEX_AI_COMPLETION_HANDLER = new GoogleVertexAiResponseHandler(
        "Google Vertex AI completion",
        GoogleVertexAiCompletionResponseEntity::fromResponse,
        GoogleVertexAiUnifiedChatCompletionResponseHandler.GoogleVertexAiErrorResponse::fromResponse,
        true
    );

    static final ResponseHandler GOOGLE_MODEL_GARDEN_ANTHROPIC_COMPLETION_HANDLER = new AnthropicResponseHandler(
        "Google Model Garden Anthropic completion",
        AnthropicChatCompletionResponseEntity::fromResponse,
        true
    );

    static final ResponseHandler GOOGLE_MODEL_GARDEN_META_COMPLETION_HANDLER = new LlamaCompletionResponseHandler(
        "Google Model Garden Meta completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    static final ResponseHandler GOOGLE_MODEL_GARDEN_HUGGING_FACE_COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        "Google Model Garden Hugging Face completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    static final ResponseHandler GOOGLE_MODEL_GARDEN_MISTRAL_COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        "Google Model Garden Mistral completion",
        OpenAiChatCompletionResponseEntity::fromResponse,
        ErrorResponse::fromResponse
    );

    static final ResponseHandler GOOGLE_MODEL_GARDEN_AI21_COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        "Google Model Garden AI21 completion",
        OpenAiChatCompletionResponseEntity::fromResponse,
        ErrorResponse::fromResponse
    );

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
        var overriddenModel = GoogleVertexAiChatCompletionModel.of(model, taskSettings);
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(COMPLETION_ERROR_PREFIX);
        GenericRequestManager<ChatCompletionInput> manager = createRequestManager(overriddenModel);

        return new SingleInputSenderExecutableAction(sender, manager, failedToSendRequestErrorMessage, COMPLETION_ERROR_PREFIX);
    }

    private GenericRequestManager<ChatCompletionInput> createRequestManager(GoogleVertexAiChatCompletionModel model) {
        switch (model.getServiceSettings().provider()) {
            case GOOGLE -> {
                return createRequestManagerWithHandler(model, GOOGLE_VERTEX_AI_COMPLETION_HANDLER);
            }
            case ANTHROPIC -> {
                return createRequestManagerWithHandler(model, GOOGLE_MODEL_GARDEN_ANTHROPIC_COMPLETION_HANDLER);
            }
            case META -> {
                return createRequestManagerWithHandler(model, GOOGLE_MODEL_GARDEN_META_COMPLETION_HANDLER);
            }
            case HUGGING_FACE -> {
                return createRequestManagerWithHandler(model, GOOGLE_MODEL_GARDEN_HUGGING_FACE_COMPLETION_HANDLER);
            }
            case MISTRAL -> {
                return createRequestManagerWithHandler(model, GOOGLE_MODEL_GARDEN_MISTRAL_COMPLETION_HANDLER);
            }
            case AI21 -> {
                return createRequestManagerWithHandler(model, GOOGLE_MODEL_GARDEN_AI21_COMPLETION_HANDLER);
            }
            case null, default -> throw new ElasticsearchException(
                "Unsupported Google Model Garden provider: " + model.getServiceSettings().provider()
            );
        }
    }

    private GenericRequestManager<ChatCompletionInput> createRequestManagerWithHandler(
        GoogleVertexAiChatCompletionModel model,
        ResponseHandler responseHandler
    ) {
        return new GenericRequestManager<>(
            serviceComponents.threadPool(),
            model,
            responseHandler,
            inputs -> new GoogleVertexAiUnifiedChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), model),
            ChatCompletionInput.class
        );
    }
}
