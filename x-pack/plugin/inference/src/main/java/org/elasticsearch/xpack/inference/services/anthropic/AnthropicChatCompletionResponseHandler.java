/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ChatCompletionErrorResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponse;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.services.anthropic.response.AnthropicChatCompletionResponseEntity;

import java.util.concurrent.Flow;

/**
 * Handles streaming chat completion responses and error parsing for Anthropic inference endpoints.
 * Adapts the AnthropicResponseHandler to support chat completion schema.
 */
public class AnthropicChatCompletionResponseHandler extends AnthropicResponseHandler {
    private static final ChatCompletionErrorResponseHandler DEFAULT_CHAT_COMPLETION_ERROR_RESPONSE_HANDLER =
        new ChatCompletionErrorResponseHandler(UnifiedChatCompletionErrorResponse.ERROR_PARSER);

    private final ChatCompletionErrorResponseHandler chatCompletionErrorResponseHandler;

    public AnthropicChatCompletionResponseHandler(String requestType) {
        this(requestType, AnthropicChatCompletionResponseEntity::fromResponse, DEFAULT_CHAT_COMPLETION_ERROR_RESPONSE_HANDLER);
    }

    private AnthropicChatCompletionResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        ChatCompletionErrorResponseHandler chatCompletionErrorResponseHandler
    ) {
        super(requestType, parseFunction, true);
        this.chatCompletionErrorResponseHandler = chatCompletionErrorResponseHandler;
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var anthropicProcessor = new AnthropicChatCompletionStreamingProcessor(
            (m, e) -> chatCompletionErrorResponseHandler.buildMidStreamChatCompletionError(request.getInferenceEntityId(), m, e)
        );
        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(anthropicProcessor);
        return new StreamingUnifiedChatCompletionResults(anthropicProcessor);
    }

    @Override
    protected UnifiedChatCompletionException buildError(String message, Request request, HttpResult result) {
        return chatCompletionErrorResponseHandler.buildChatCompletionError(message, request, result);
    }
}
