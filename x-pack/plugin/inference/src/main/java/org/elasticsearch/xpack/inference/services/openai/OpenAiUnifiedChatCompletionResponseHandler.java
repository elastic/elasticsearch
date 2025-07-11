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
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.streaming.OpenAiStreamingChatCompletionErrorResponse;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;

import java.util.concurrent.Flow;
import java.util.function.Function;

/**
 * Handles streaming chat completion responses and error parsing for OpenAI inference endpoints.
 * This handler is designed to work with the unified OpenAI chat completion API.
 */
public class OpenAiUnifiedChatCompletionResponseHandler extends OpenAiChatCompletionResponseHandler {
    public OpenAiUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, OpenAiStreamingChatCompletionErrorResponse::fromResponse);
    }

    public OpenAiUnifiedChatCompletionResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        Function<HttpResult, ErrorResponse> errorParseFunction
    ) {
        super(requestType, parseFunction, errorParseFunction);
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var openAiProcessor = new OpenAiUnifiedStreamingProcessor(
            (m, e) -> buildMidStreamChatCompletionError(request.getInferenceEntityId(), m, e)
        );
        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(openAiProcessor);
        return new StreamingUnifiedChatCompletionResults(openAiProcessor);
    }

    @Override
    protected UnifiedChatCompletionException buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        return buildChatCompletionError(message, request, result, errorResponse);
    }

    /**
     * Builds a custom mid-stream {@link UnifiedChatCompletionException} for OpenAI inference endpoints.
     * This method is called when an error response is received during streaming.
     *
     * @param inferenceEntityId the ID of the inference entity
     * @param message the error message received during streaming
     * @param e the exception that occurred
     * @return an instance of {@link UnifiedChatCompletionException} with details from the error response
     */
    public UnifiedChatCompletionException buildMidStreamChatCompletionError(String inferenceEntityId, String message, Exception e) {
        // Use the custom type StreamingErrorResponse for mid-stream errors
        return buildMidStreamChatCompletionError(inferenceEntityId, message, e, OpenAiStreamingChatCompletionErrorResponse::fromString);
    }
}
