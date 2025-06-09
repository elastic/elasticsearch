/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.mistral.response.MistralErrorResponse;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import java.util.Locale;

/**
 * Handles streaming chat completion responses and error parsing for Mistral inference endpoints.
 * Adapts the OpenAI handler to support Mistral's error schema.
 */
public class MistralUnifiedChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String MISTRAL_ERROR = "mistral_error";

    public MistralUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, MistralErrorResponse::fromResponse);
    }

    @Override
    protected UnifiedChatCompletionException buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        return buildChatCompletionError(message, request, result, errorResponse, MistralErrorResponse.class);
    }

    @Override
    protected UnifiedChatCompletionException buildProviderSpecificChatCompletionError(
        ErrorResponse errorResponse,
        String errorMessage,
        RestStatus restStatus
    ) {
        return new UnifiedChatCompletionException(restStatus, errorMessage, MISTRAL_ERROR, restStatus.name().toLowerCase(Locale.ROOT));
    }
}
