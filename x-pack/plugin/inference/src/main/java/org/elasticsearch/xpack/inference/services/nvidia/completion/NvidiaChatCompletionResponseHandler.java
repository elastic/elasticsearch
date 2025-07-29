/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.completion;

import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.llama.response.LlamaErrorResponse;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import java.util.Locale;

/**
 * Handles streaming chat completion responses and error parsing for Nvidia inference endpoints.
 * This handler is designed to work with the unified Nvidia chat completion API.
 */
public class NvidiaChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String NVIDIA_ERROR = "nvidia_error";

    /**
     * Constructor for creating a NvidiaChatCompletionResponseHandler with specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public NvidiaChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, LlamaErrorResponse::fromResponse);
    }

    /**
     * Constructor for creating a NvidiaChatCompletionResponseHandler with specified request type,
     * @param message the error message to include in the exception
     * @param request the request that caused the error
     * @param result the HTTP result containing the response
     * @param errorResponse the error response parsed from the HTTP result
     * @return an exception representing the error, specific to Nvidia chat completion
     */
    @Override
    protected Exception buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        assert request.isStreaming() : "Only streaming requests support this format";
        var responseStatusCode = result.response().getStatusLine().getStatusCode();
        if (request.isStreaming()) {
            var errorMessage = constructErrorMessage(message, request, errorResponse, responseStatusCode);
            var restStatus = toRestStatus(responseStatusCode);
            return errorResponse instanceof LlamaErrorResponse
                ? new UnifiedChatCompletionException(restStatus, errorMessage, NVIDIA_ERROR, restStatus.name().toLowerCase(Locale.ROOT))
                : new UnifiedChatCompletionException(
                    restStatus,
                    errorMessage,
                    createErrorType(errorResponse),
                    restStatus.name().toLowerCase(Locale.ROOT)
                );
        } else {
            return super.buildError(message, request, result, errorResponse);
        }
    }

}
