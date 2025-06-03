/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceErrorResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedStreamingProcessor;

import java.util.Locale;
import java.util.concurrent.Flow;

import static org.elasticsearch.core.Strings.format;

/**
 * Handles streaming chat completion responses and error parsing for Elastic Inference Service endpoints.
 * This handler is designed to work with the unified Elastic Inference Service chat completion API.
 */
public class ElasticInferenceServiceUnifiedChatCompletionResponseHandler extends ElasticInferenceServiceResponseHandler {
    public ElasticInferenceServiceUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, true);
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        // EIS uses the unified API spec
        var openAiProcessor = new OpenAiUnifiedStreamingProcessor(
            (m, e) -> buildMidStreamChatCompletionError(request.getInferenceEntityId(), m, e)
        );

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(openAiProcessor);
        return new StreamingUnifiedChatCompletionResults(openAiProcessor);
    }

    /**
     * Builds an error for the Elastic Inference Service.
     * This method is called when an error response is received from the service.
     *
     * @param message The error message to include in the exception.
     * @param request The request that caused the error.
     * @param result The HTTP result containing the error response.
     * @param errorResponse The parsed error response from the service.
     * @return An instance of {@link Exception} representing the error.
     */
    @Override
    protected UnifiedChatCompletionException buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        return buildChatCompletionError(message, request, result, errorResponse, ErrorResponse.class);
    }

    /**
     * Builds a custom {@link UnifiedChatCompletionException} for the Elastic Inference Service.
     * This method is called when an error response is received from the service.
     *
     * @param errorResponse The error response received from the service.
     * @param errorMessage The error message to include in the exception.
     * @param restStatus The HTTP status of the error response.
     * @param errorResponseClass The class of the error response.
     * @return An instance of {@link UnifiedChatCompletionException} with details from the error response.
     */
    @Override
    protected UnifiedChatCompletionException buildChatCompletionError(
        ErrorResponse errorResponse,
        String errorMessage,
        RestStatus restStatus,
        Class<? extends ErrorResponse> errorResponseClass
    ) {
        return new UnifiedChatCompletionException(restStatus, errorMessage, ERROR_TYPE, restStatus.name().toLowerCase(Locale.ROOT));
    }

    /**
     * Builds a mid-stream error for the Elastic Inference Service.
     * This method is called when an error occurs during the streaming process.
     *
     * @param inferenceEntityId The ID of the inference entity.
     * @param message The error message received from the service.
     * @param e The exception that occurred, if any.
     * @return An instance of {@link UnifiedChatCompletionException} representing the mid-stream error.
     */
    @Override
    public UnifiedChatCompletionException buildMidStreamChatCompletionError(String inferenceEntityId, String message, Exception e) {
        var errorResponse = extractMidStreamChatCompletionErrorResponse(message);
        // Check if the error response contains a specific structure
        if (errorResponse.errorStructureFound()) {
            return buildProviderSpecificMidStreamChatCompletionError(inferenceEntityId, errorResponse);
        } else if (e != null) {
            return UnifiedChatCompletionException.fromThrowable(e);
        } else {
            return buildDefaultMidStreamChatCompletionError(inferenceEntityId, errorResponse);
        }
    }

    /**
     * Extracts the error response from the message. This method is specific to the Elastic Inference Service
     * and should parse the message according to its error response format.
     *
     * @param message The message containing the error response.
     * @return An instance of {@link ErrorResponse} parsed from the message.
     */
    @Override
    protected ErrorResponse extractMidStreamChatCompletionErrorResponse(String message) {
        return ElasticInferenceServiceErrorResponseEntity.fromString(message);
    }

    /**
     * Builds a custom mid-stream {@link UnifiedChatCompletionException} for the Elastic Inference Service.
     * This method is called when a specific error response structure is found in the message.
     *
     * @param inferenceEntityId The ID of the inference entity.
     * @param errorResponse The error response parsed from the message.
     * @return An instance of {@link UnifiedChatCompletionException} with details from the error response.
     */
    @Override
    protected UnifiedChatCompletionException buildProviderSpecificMidStreamChatCompletionError(
        String inferenceEntityId,
        ErrorResponse errorResponse
    ) {
        return new UnifiedChatCompletionException(
            RestStatus.INTERNAL_SERVER_ERROR,
            format(
                "%s for request from inference entity id [%s]. Error message: [%s]",
                SERVER_ERROR_OBJECT,
                inferenceEntityId,
                errorResponse.getErrorMessage()
            ),
            ERROR_TYPE,
            STREAM_ERROR
        );
    }

    /**
     * Builds a default mid-stream {@link UnifiedChatCompletionException} for the Elastic Inference Service.
     * This method is called when specific error response structure is NOT found in the message.
     *
     * @param inferenceEntityId The ID of the inference entity.
     * @param errorResponse The error response parsed from the message.
     * @return An instance of {@link UnifiedChatCompletionException} with a generic error message.
     */
    @Override
    protected UnifiedChatCompletionException buildDefaultMidStreamChatCompletionError(
        String inferenceEntityId,
        ErrorResponse errorResponse
    ) {
        return new UnifiedChatCompletionException(
            RestStatus.INTERNAL_SERVER_ERROR,
            format("%s for request from inference entity id [%s]", SERVER_ERROR_OBJECT, inferenceEntityId),
            ERROR_TYPE,
            STREAM_ERROR
        );
    }
}
