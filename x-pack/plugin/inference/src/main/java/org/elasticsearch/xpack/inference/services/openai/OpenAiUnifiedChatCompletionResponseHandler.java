/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ChatCompletionErrorResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponse;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;

import java.util.concurrent.Flow;
import java.util.function.Function;

/**
 * Handles streaming chat completion responses and error parsing for OpenAI inference endpoints.
 * This handler is designed to work with the unified OpenAI chat completion API.
 */
public class OpenAiUnifiedChatCompletionResponseHandler extends OpenAiChatCompletionResponseHandler {
    private static final ChatCompletionErrorResponseHandler DEFAULT_CHAT_COMPLETION_ERROR_RESPONSE_HANDLER =
        new ChatCompletionErrorResponseHandler(UnifiedChatCompletionErrorResponse.ERROR_PARSER);

    private final ChatCompletionErrorResponseHandler chatCompletionErrorResponseHandler;

    public OpenAiUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        this(
            requestType,
            parseFunction,
            UnifiedChatCompletionErrorResponse::fromHttpResult,
            DEFAULT_CHAT_COMPLETION_ERROR_RESPONSE_HANDLER
        );
    }

    public OpenAiUnifiedChatCompletionResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        Function<HttpResult, ErrorResponse> errorParseFunction,
        UnifiedChatCompletionErrorParserContract unifiedChatCompletionErrorParser
    ) {
        super(requestType, parseFunction, errorParseFunction);
        this.chatCompletionErrorResponseHandler = new ChatCompletionErrorResponseHandler(unifiedChatCompletionErrorParser);
    }

    private OpenAiUnifiedChatCompletionResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        Function<HttpResult, ErrorResponse> errorParseFunction,
        ChatCompletionErrorResponseHandler chatCompletionErrorResponseHandler
    ) {
        super(requestType, parseFunction, errorParseFunction);
        this.chatCompletionErrorResponseHandler = chatCompletionErrorResponseHandler;
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var openAiProcessor = new OpenAiUnifiedStreamingProcessor(
            (m, e) -> chatCompletionErrorResponseHandler.buildMidStreamChatCompletionError(request.getInferenceEntityId(), m, e)
        );
        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(openAiProcessor);
        return new StreamingUnifiedChatCompletionResults(openAiProcessor);
    }

    @Override
    protected UnifiedChatCompletionException buildError(String message, Request request, HttpResult result) {
        return chatCompletionErrorResponseHandler.buildChatCompletionError(message, request, result);
    }

    public static UnifiedChatCompletionException buildMidStreamError(String inferenceEntityId, String message, Exception e) {
        return DEFAULT_CHAT_COMPLETION_ERROR_RESPONSE_HANDLER.buildMidStreamChatCompletionError(inferenceEntityId, message, e);
    }
}
