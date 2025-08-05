/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.completion;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import java.util.Locale;

import static org.elasticsearch.core.Strings.format;

/**
 * Handles streaming chat completion responses and error parsing for AI21 inference endpoints.
 * Adapts the OpenAI handler to support AI21's error schema.
 */
public class Ai21ChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String AI_21_ERROR = "ai21_error";

    public Ai21ChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, ErrorResponse::fromResponse);
    }

    @Override
    protected Exception buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        assert request.isStreaming() : "Only streaming requests support this format";
        var responseStatusCode = result.response().getStatusLine().getStatusCode();
        var errorMessage = constructErrorMessage(message, request, errorResponse, responseStatusCode);
        var restStatus = toRestStatus(responseStatusCode);
        return new UnifiedChatCompletionException(restStatus, errorMessage, AI_21_ERROR, restStatus.name().toLowerCase(Locale.ROOT));
    }

    protected Exception buildMidStreamError(Request request, String message, Exception e) {
        var errorResponse = ErrorResponse.fromString(message);
        if (errorResponse.errorStructureFound()) {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format(
                    "%s for request from inference entity id [%s]. Error message: [%s]",
                    SERVER_ERROR_OBJECT,
                    request.getInferenceEntityId(),
                    errorResponse.getErrorMessage()
                ),
                AI_21_ERROR,
                "stream_error"
            );
        } else if (e != null) {
            return UnifiedChatCompletionException.fromThrowable(e);
        } else {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format("%s for request from inference entity id [%s]", SERVER_ERROR_OBJECT, request.getInferenceEntityId()),
                createErrorType(errorResponse),
                "stream_error"
            );
        }
    }
}
