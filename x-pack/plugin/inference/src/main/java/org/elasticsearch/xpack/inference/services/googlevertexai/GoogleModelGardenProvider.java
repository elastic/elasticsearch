/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.ai21.request.Ai21ChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicResponseHandler;
import org.elasticsearch.xpack.inference.services.anthropic.response.AnthropicChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.completion.GoogleModelGardenAnthropicChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.completion.GoogleVertexAiUnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.googlevertexai.response.GoogleVertexAiCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.huggingface.request.completion.HuggingFaceUnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.llama.request.completion.LlamaChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.mistral.MistralUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.mistral.request.completion.MistralChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;

import java.util.Locale;

/**
 * Enum representing the supported model garden providers.
 */
public enum GoogleModelGardenProvider {
    GOOGLE,
    ANTHROPIC,
    META,
    HUGGING_FACE,
    MISTRAL,
    AI21;

    private static final ResponseHandler GOOGLE_VERTEX_AI_COMPLETION_HANDLER = new GoogleVertexAiResponseHandler(
        "Google Vertex AI completion",
        GoogleVertexAiCompletionResponseEntity::fromResponse,
        GoogleVertexAiUnifiedChatCompletionResponseHandler.GoogleVertexAiErrorResponse::fromResponse,
        true
    );

    private static final ResponseHandler GOOGLE_MODEL_GARDEN_ANTHROPIC_COMPLETION_HANDLER = new AnthropicResponseHandler(
        "Google Model Garden Anthropic completion",
        AnthropicChatCompletionResponseEntity::fromResponse,
        true
    );

    private static final ResponseHandler GOOGLE_MODEL_GARDEN_META_COMPLETION_HANDLER = new LlamaCompletionResponseHandler(
        "Google Model Garden Meta completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private static final ResponseHandler GOOGLE_MODEL_GARDEN_HUGGING_FACE_COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        "Google Model Garden Hugging Face completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private static final ResponseHandler GOOGLE_MODEL_GARDEN_MISTRAL_COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        "Google Model Garden Mistral completion",
        OpenAiChatCompletionResponseEntity::fromResponse,
        ErrorResponse::fromResponse
    );

    private static final ResponseHandler GOOGLE_MODEL_GARDEN_AI21_COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        "Google Model Garden AI21 completion",
        OpenAiChatCompletionResponseEntity::fromResponse,
        ErrorResponse::fromResponse
    );

    private static final ResponseHandler GOOGLE_VERTEX_AI_CHAT_COMPLETION_HANDLER = new GoogleVertexAiUnifiedChatCompletionResponseHandler(
        "Google Vertex AI chat completion"
    );

    private static final ResponseHandler ANTHROPIC_CHAT_COMPLETION_HANDLER = new AnthropicChatCompletionResponseHandler(
        "Google Model Garden Anthropic chat completion"
    );

    private static final ResponseHandler META_CHAT_COMPLETION_HANDLER = new LlamaChatCompletionResponseHandler(
        "Google Model Garden Meta chat completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private static final ResponseHandler HUGGING_FACE_CHAT_COMPLETION_HANDLER = new HuggingFaceChatCompletionResponseHandler(
        "Google Model Garden Hugging Face chat completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private static final ResponseHandler MISTRAL_CHAT_COMPLETION_HANDLER = new MistralUnifiedChatCompletionResponseHandler(
        "Google Model Garden Mistral chat completions",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private static final ResponseHandler AI21_CHAT_COMPLETION_HANDLER = new Ai21ChatCompletionResponseHandler(
        "Google Model Garden Ai21 chat completions",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    /**
     * Gets the completion response handler for the model garden provider.
     * @return the ResponseHandler associated with the provider
     */
    public ResponseHandler getCompletionResponseHandler() {
        return switch (this) {
            case GOOGLE -> GOOGLE_VERTEX_AI_COMPLETION_HANDLER;
            case ANTHROPIC -> GOOGLE_MODEL_GARDEN_ANTHROPIC_COMPLETION_HANDLER;
            case META -> GOOGLE_MODEL_GARDEN_META_COMPLETION_HANDLER;
            case HUGGING_FACE -> GOOGLE_MODEL_GARDEN_HUGGING_FACE_COMPLETION_HANDLER;
            case MISTRAL -> GOOGLE_MODEL_GARDEN_MISTRAL_COMPLETION_HANDLER;
            case AI21 -> GOOGLE_MODEL_GARDEN_AI21_COMPLETION_HANDLER;
        };
    }

    /**
     * Gets the chat completion response handler for the model garden provider.
     * @return the ResponseHandler associated with the provider
     */
    public ResponseHandler getChatCompletionResponseHandler() {
        return switch (this) {
            case GOOGLE -> GOOGLE_VERTEX_AI_CHAT_COMPLETION_HANDLER;
            case ANTHROPIC -> ANTHROPIC_CHAT_COMPLETION_HANDLER;
            case META -> META_CHAT_COMPLETION_HANDLER;
            case HUGGING_FACE -> HUGGING_FACE_CHAT_COMPLETION_HANDLER;
            case MISTRAL -> MISTRAL_CHAT_COMPLETION_HANDLER;
            case AI21 -> AI21_CHAT_COMPLETION_HANDLER;
        };
    }

    /**
     * Creates the request entity for the model garden provider based on the unified chat input and model ID.
     * @param unifiedChatInput the unified chat input containing messages and parameters for the chat completion request
     * @param modelId the model ID to be used for the request
     * @param taskSettings the task settings specific to Google Vertex AI chat completion
     * @return a ToXContentObject representing the request entity for the provider
     */
    public ToXContentObject createRequestEntity(
        UnifiedChatInput unifiedChatInput,
        String modelId,
        GoogleVertexAiChatCompletionTaskSettings taskSettings
    ) {
        return switch (this) {
            case GOOGLE -> new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput, taskSettings.thinkingConfig());
            case ANTHROPIC -> new GoogleModelGardenAnthropicChatCompletionRequestEntity(unifiedChatInput, taskSettings);
            case META -> new LlamaChatCompletionRequestEntity(unifiedChatInput, modelId);
            case HUGGING_FACE -> new HuggingFaceUnifiedChatCompletionRequestEntity(unifiedChatInput, modelId);
            case MISTRAL -> new MistralChatCompletionRequestEntity(unifiedChatInput, modelId);
            case AI21 -> new Ai21ChatCompletionRequestEntity(unifiedChatInput, modelId);
        };
    }

    public static GoogleModelGardenProvider fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
