/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.SERVER_ERROR_OBJECT;
import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.toRestStatus;

public class ChatCompletionErrorResponseHandler {
    private static final String STREAM_ERROR = "stream_error";

    private final UnifiedChatCompletionErrorParser unifiedChatCompletionErrorParser;

    public ChatCompletionErrorResponseHandler(UnifiedChatCompletionErrorParser errorParser) {
        this.unifiedChatCompletionErrorParser = Objects.requireNonNull(errorParser);
    }

    public void checkForErrorObject(Request request, HttpResult result) {
        var errorEntity = unifiedChatCompletionErrorParser.parse(result);

        if (errorEntity.errorStructureFound()) {
            // We don't really know what happened because the status code was 200 so we'll return a failure and let the
            // client retry if necessary
            // If we did want to retry here, we'll need to determine if this was a streaming request, if it was
            // we shouldn't retry because that would replay the entire streaming request and the client would get
            // duplicate chunks back
            throw new RetryException(false, buildChatCompletionErrorInternal(SERVER_ERROR_OBJECT, request, result, errorEntity));
        }
    }

    public UnifiedChatCompletionException buildChatCompletionError(String message, Request request, HttpResult result) {
        var errorResponse = unifiedChatCompletionErrorParser.parse(result);
        return buildChatCompletionErrorInternal(message, request, result, errorResponse);
    }

    /**
     * Returns an exception that adheres to the chat completion error response format.
     *
     * @param message            the error message to include in the exception
     * @param request            the request that caused the error
     * @param result             the HTTP result containing the error response
     * @param errorResponse      the parsed error response from the HTTP result
     * @return an instance of {@link UnifiedChatCompletionException} with details from the error response
     */
    private UnifiedChatCompletionException buildChatCompletionErrorInternal(
        String message,
        Request request,
        HttpResult result,
        UnifiedChatCompletionErrorResponse errorResponse
    ) {
        assert request.isStreaming() : "Only streaming requests support this format";
        var statusCode = result.response().getStatusLine().getStatusCode();
        var errorMessage = BaseResponseHandler.constructErrorMessage(message, request, errorResponse, statusCode);
        var restStatus = toRestStatus(statusCode);

        if (errorResponse.errorStructureFound()) {
            return new UnifiedChatCompletionException(
                restStatus,
                errorMessage,
                errorResponse.type(),
                errorResponse.code(),
                errorResponse.param()
            );
        } else {
            return buildDefaultChatCompletionError(errorResponse, errorMessage, restStatus);
        }
    }

    /**
     * Builds a default {@link UnifiedChatCompletionException} for a streaming request.
     * This method is used when an error response is received but no specific error handling is implemented.
     * Only streaming requests should use this method.
     *
     * @param errorResponse the error response parsed from the HTTP result
     * @param errorMessage  the error message to include in the exception
     * @param restStatus    the REST status code of the response
     * @return an instance of {@link UnifiedChatCompletionException} with details from the error response
     */
    public static UnifiedChatCompletionException buildDefaultChatCompletionError(
        ErrorResponse errorResponse,
        String errorMessage,
        RestStatus restStatus
    ) {
        return new UnifiedChatCompletionException(
            restStatus,
            errorMessage,
            createErrorType(errorResponse),
            restStatus.name().toLowerCase(Locale.ROOT)
        );
    }

    /**
     * Builds a mid-stream error for a streaming request with a custom error type.
     * This method is used when an error occurs while processing a streaming response and allows for custom error handling.
     * Only streaming requests should use this method.
     *
     * @param inferenceEntityId the ID of the inference entity
     * @param message           the error message
     * @param e                the exception that caused the error, can be null
     * @return a {@link UnifiedChatCompletionException} representing the mid-stream error
     */
    public UnifiedChatCompletionException buildMidStreamChatCompletionError(String inferenceEntityId, String message, Exception e) {
        var error = unifiedChatCompletionErrorParser.parse(message);

        if (error.errorStructureFound()) {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format(
                    "%s for request from inference entity id [%s]. Error message: [%s]",
                    SERVER_ERROR_OBJECT,
                    inferenceEntityId,
                    error.getErrorMessage()
                ),
                error.type(),
                error.code(),
                error.param()
            );
        } else if (e != null) {
            // If the error response does not match, we can still return an exception based on the original throwable
            return UnifiedChatCompletionException.fromThrowable(e);
        } else {
            // If no specific error response is found, we return a default mid-stream error
            return buildDefaultMidStreamChatCompletionError(inferenceEntityId, error);
        }
    }

    /**
     * Builds a default mid-stream error for a streaming request.
     * This method is used when no specific error response is found in the message.
     * Only streaming requests should use this method.
     *
     * @param inferenceEntityId the ID of the inference entity
     * @param errorResponse     the error response extracted from the message
     * @return a {@link UnifiedChatCompletionException} representing the default mid-stream error
     */
    public static UnifiedChatCompletionException buildDefaultMidStreamChatCompletionError(
        String inferenceEntityId,
        ErrorResponse errorResponse
    ) {
        return new UnifiedChatCompletionException(
            RestStatus.INTERNAL_SERVER_ERROR,
            format("%s for request from inference entity id [%s]", SERVER_ERROR_OBJECT, inferenceEntityId),
            createErrorType(errorResponse),
            STREAM_ERROR
        );
    }

    /**
     * Creates a string representation of the error type based on the provided ErrorResponse.
     * This method is used to generate a human-readable error type for logging or exception messages.
     *
     * @param errorResponse the ErrorResponse object
     * @return a string representing the error type
     */
    private static String createErrorType(ErrorResponse errorResponse) {
        return errorResponse != null ? errorResponse.getClass().getSimpleName() : "unknown";
    }
}
