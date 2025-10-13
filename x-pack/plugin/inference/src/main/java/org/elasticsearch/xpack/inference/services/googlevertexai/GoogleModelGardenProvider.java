/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.core.Nullable;
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
    GOOGLE(
        CompletionResponseHandlerHolder.GOOGLE_VERTEX_AI_COMPLETION_HANDLER,
        ChatCompletionResponseHandlerHolder.GOOGLE_VERTEX_AI_CHAT_COMPLETION_HANDLER,
        (unifiedChatInput, modelId, taskSettings) -> new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            taskSettings.thinkingConfig()
        )
    ),
    ANTHROPIC(
        CompletionResponseHandlerHolder.ANTHROPIC_COMPLETION_HANDLER,
        ChatCompletionResponseHandlerHolder.ANTHROPIC_CHAT_COMPLETION_HANDLER,
        (unifiedChatInput, modelId, taskSettings) -> new GoogleModelGardenAnthropicChatCompletionRequestEntity(
            unifiedChatInput,
            taskSettings
        )
    ),
    META(
        CompletionResponseHandlerHolder.META_COMPLETION_HANDLER,
        ChatCompletionResponseHandlerHolder.META_CHAT_COMPLETION_HANDLER,
        (unifiedChatInput, modelId, taskSettings) -> new LlamaChatCompletionRequestEntity(unifiedChatInput, modelId)
    ),
    HUGGING_FACE(
        CompletionResponseHandlerHolder.HUGGING_FACE_COMPLETION_HANDLER,
        ChatCompletionResponseHandlerHolder.HUGGING_FACE_CHAT_COMPLETION_HANDLER,
        (unifiedChatInput, modelId, taskSettings) -> new HuggingFaceUnifiedChatCompletionRequestEntity(unifiedChatInput, modelId)
    ),
    MISTRAL(
        CompletionResponseHandlerHolder.MISTRAL_COMPLETION_HANDLER,
        ChatCompletionResponseHandlerHolder.MISTRAL_CHAT_COMPLETION_HANDLER,
        (unifiedChatInput, modelId, taskSettings) -> new MistralChatCompletionRequestEntity(unifiedChatInput, modelId)
    ),
    AI21(
        CompletionResponseHandlerHolder.AI21_COMPLETION_HANDLER,
        ChatCompletionResponseHandlerHolder.AI21_CHAT_COMPLETION_HANDLER,
        (unifiedChatInput, modelId, taskSettings) -> new Ai21ChatCompletionRequestEntity(unifiedChatInput, modelId)
    );

    private final ResponseHandler completionResponseHandler;
    private final ResponseHandler chatCompletionResponseHandler;
    private final RequestEntityCreator entityCreator;

    GoogleModelGardenProvider(
        ResponseHandler completionResponseHandler,
        ResponseHandler chatCompletionResponseHandler,
        RequestEntityCreator entityCreator
    ) {
        this.completionResponseHandler = completionResponseHandler;
        this.chatCompletionResponseHandler = chatCompletionResponseHandler;
        this.entityCreator = entityCreator;
    }

    public ResponseHandler getCompletionResponseHandler() {
        return completionResponseHandler;
    }

    public ResponseHandler getChatCompletionResponseHandler() {
        return chatCompletionResponseHandler;
    }

    public ToXContentObject createRequestEntity(
        UnifiedChatInput unifiedChatInput,
        @Nullable String modelId,
        GoogleVertexAiChatCompletionTaskSettings taskSettings
    ) {
        return entityCreator.create(unifiedChatInput, modelId, taskSettings);
    }

    private static class CompletionResponseHandlerHolder {
        static final ResponseHandler GOOGLE_VERTEX_AI_COMPLETION_HANDLER = new GoogleVertexAiResponseHandler(
            "Google Vertex AI completion",
            GoogleVertexAiCompletionResponseEntity::fromResponse,
            GoogleVertexAiUnifiedChatCompletionResponseHandler.GoogleVertexAiErrorResponse::fromResponse,
            true
        );

        static final ResponseHandler ANTHROPIC_COMPLETION_HANDLER = new AnthropicResponseHandler(
            "Google Model Garden Anthropic completion",
            AnthropicChatCompletionResponseEntity::fromResponse,
            true
        );

        static final ResponseHandler META_COMPLETION_HANDLER = new LlamaCompletionResponseHandler(
            "Google Model Garden Meta completion",
            OpenAiChatCompletionResponseEntity::fromResponse
        );

        static final ResponseHandler HUGGING_FACE_COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
            "Google Model Garden Hugging Face completion",
            OpenAiChatCompletionResponseEntity::fromResponse
        );

        static final ResponseHandler MISTRAL_COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
            "Google Model Garden Mistral completion",
            OpenAiChatCompletionResponseEntity::fromResponse,
            ErrorResponse::fromResponse
        );

        static final ResponseHandler AI21_COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
            "Google Model Garden AI21 completion",
            OpenAiChatCompletionResponseEntity::fromResponse,
            ErrorResponse::fromResponse
        );
    }

    private static class ChatCompletionResponseHandlerHolder {
        static final ResponseHandler GOOGLE_VERTEX_AI_CHAT_COMPLETION_HANDLER = new GoogleVertexAiUnifiedChatCompletionResponseHandler(
            "Google Vertex AI chat completion"
        );

        static final ResponseHandler ANTHROPIC_CHAT_COMPLETION_HANDLER = new AnthropicChatCompletionResponseHandler(
            "Google Model Garden Anthropic chat completion"
        );

        static final ResponseHandler META_CHAT_COMPLETION_HANDLER = new LlamaChatCompletionResponseHandler(
            "Google Model Garden Meta chat completion",
            OpenAiChatCompletionResponseEntity::fromResponse
        );

        static final ResponseHandler HUGGING_FACE_CHAT_COMPLETION_HANDLER = new HuggingFaceChatCompletionResponseHandler(
            "Google Model Garden Hugging Face chat completion",
            OpenAiChatCompletionResponseEntity::fromResponse
        );

        static final ResponseHandler MISTRAL_CHAT_COMPLETION_HANDLER = new MistralUnifiedChatCompletionResponseHandler(
            "Google Model Garden Mistral chat completions",
            OpenAiChatCompletionResponseEntity::fromResponse
        );

        static final ResponseHandler AI21_CHAT_COMPLETION_HANDLER = new Ai21ChatCompletionResponseHandler(
            "Google Model Garden AI21 chat completions",
            OpenAiChatCompletionResponseEntity::fromResponse
        );
    }

    @FunctionalInterface
    private interface RequestEntityCreator {
        ToXContentObject create(
            UnifiedChatInput unifiedChatInput,
            @Nullable String modelId,
            GoogleVertexAiChatCompletionTaskSettings taskSettings
        );
    }

    public static GoogleModelGardenProvider fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
