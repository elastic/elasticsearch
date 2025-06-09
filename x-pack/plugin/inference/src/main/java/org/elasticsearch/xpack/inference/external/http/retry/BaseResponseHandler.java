/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;

public abstract class BaseResponseHandler implements ResponseHandler {

    public static final String SERVER_ERROR = "Received a server error status code";
    public static final String RATE_LIMIT = "Received a rate limit status code";
    public static final String AUTHENTICATION = "Received an authentication error status code";
    public static final String PERMISSION_DENIED = "Received a permission denied error status code";
    public static final String REDIRECTION = "Unhandled redirection";
    public static final String CONTENT_TOO_LARGE = "Received a content too large status code";
    public static final String UNSUCCESSFUL = "Received an unsuccessful status code";
    public static final String SERVER_ERROR_OBJECT = "Received an error response";
    public static final String BAD_REQUEST = "Received a bad request status code";
    public static final String METHOD_NOT_ALLOWED = "Received a method not allowed status code";
    protected static final String ERROR_TYPE = "error";
    protected static final String STREAM_ERROR = "stream_error";

    protected final String requestType;
    protected final ResponseParser parseFunction;
    private final Function<HttpResult, ErrorResponse> errorParseFunction;
    private final boolean canHandleStreamingResponses;

    protected BaseResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        Function<HttpResult, ErrorResponse> errorParseFunction
    ) {
        this(requestType, parseFunction, errorParseFunction, false);
    }

    protected BaseResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        Function<HttpResult, ErrorResponse> errorParseFunction,
        boolean canHandleStreamingResponses
    ) {
        this.requestType = Objects.requireNonNull(requestType);
        this.parseFunction = Objects.requireNonNull(parseFunction);
        this.errorParseFunction = Objects.requireNonNull(errorParseFunction);
        this.canHandleStreamingResponses = canHandleStreamingResponses;
    }

    @Override
    public boolean canHandleStreamingResponses() {
        return canHandleStreamingResponses;
    }

    @Override
    public InferenceServiceResults parseResult(Request request, HttpResult result) throws RetryException {
        try {
            return parseFunction.apply(request, result);
        } catch (Exception e) {
            throw new RetryException(true, e);
        }
    }

    @Override
    public String getRequestType() {
        return requestType;
    }

    @Override
    public void validateResponse(
        ThrottlerManager throttlerManager,
        Logger logger,
        Request request,
        HttpResult result,
        boolean checkForErrorObject
    ) {
        checkForFailureStatusCode(request, result);
        checkForEmptyBody(throttlerManager, logger, request, result);

        if (checkForErrorObject) {
            // When the response is streamed the status code could be 200 but the error object will be set
            // so we need to check for that specifically
            checkForErrorObject(request, result);
        }
    }

    protected abstract void checkForFailureStatusCode(Request request, HttpResult result);

    private void checkForErrorObject(Request request, HttpResult result) {
        var errorEntity = errorParseFunction.apply(result);

        if (errorEntity.errorStructureFound()) {
            // We don't really know what happened because the status code was 200 so we'll return a failure and let the
            // client retry if necessary
            // If we did want to retry here, we'll need to determine if this was a streaming request, if it was
            // we shouldn't retry because that would replay the entire streaming request and the client would get
            // duplicate chunks back
            throw new RetryException(false, buildError(SERVER_ERROR_OBJECT, request, result, errorEntity));
        }
    }

    protected Exception buildError(String message, Request request, HttpResult result) {
        var errorResponse = errorParseFunction.apply(result);
        return buildError(message, request, result, errorResponse);
    }

    protected Exception buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        var responseStatusCode = result.response().getStatusLine().getStatusCode();
        return new ElasticsearchStatusException(
            errorMessage(message, request, errorResponse, responseStatusCode),
            toRestStatus(responseStatusCode)
        );
    }

    /**
     * Builds an error for a streaming request with a custom error type.
     * This method is used when an error response is received from the external service.
     * Only streaming requests support this format, and it should be used when the error response.
     *
     * @param message            the error message to include in the exception
     * @param request            the request that caused the error
     * @param result             the HTTP result containing the error response
     * @param errorResponse      the parsed error response from the HTTP result
     * @param errorResponseClass the class of the expected error response type
     * @return an instance of {@link UnifiedChatCompletionException} with details from the error response
     */
    protected UnifiedChatCompletionException buildChatCompletionError(
        String message,
        Request request,
        HttpResult result,
        ErrorResponse errorResponse,
        Class<? extends ErrorResponse> errorResponseClass
    ) {
        assert request.isStreaming() : "Only streaming requests support this format";
        var statusCode = result.response().getStatusLine().getStatusCode();
        var errorMessage = errorMessage(message, request, errorResponse, statusCode);
        var restStatus = toRestStatus(statusCode);

        return buildChatCompletionError(errorResponse, errorMessage, restStatus, errorResponseClass);
    }

    /**
     * Builds a {@link UnifiedChatCompletionException} for a streaming request.
     * This method is used when an error response is received from the external service.
     * Only streaming requests should use this method.
     *
     * @param errorResponse      the error response parsed from the HTTP result
     * @param errorMessage       the error message to include in the exception
     * @param restStatus         the REST status code of the response
     * @param errorResponseClass the class of the expected error response type
     * @return an instance of {@link UnifiedChatCompletionException} with details from the error response
     */
    protected UnifiedChatCompletionException buildChatCompletionError(
        ErrorResponse errorResponse,
        String errorMessage,
        RestStatus restStatus,
        Class<? extends ErrorResponse> errorResponseClass
    ) {
        if (errorResponseClass.isInstance(errorResponse)) {
            return buildProviderSpecificChatCompletionError(errorResponse, errorMessage, restStatus);
        } else {
            return buildDefaultChatCompletionError(errorResponse, errorMessage, restStatus);
        }
    }

    /**
     * Builds a custom {@link UnifiedChatCompletionException} for a streaming request.
     * This method is called when a specific error response is found in the HTTP result.
     * It must be implemented by subclasses to handle specific error response formats.
     * Only streaming requests should use this method.
     *
     * @param errorResponse the error response parsed from the HTTP result
     * @param errorMessage  the error message to include in the exception
     * @param restStatus    the REST status code of the response
     * @return an instance of {@link UnifiedChatCompletionException} with details from the error response
     */
    protected UnifiedChatCompletionException buildProviderSpecificChatCompletionError(
        ErrorResponse errorResponse,
        String errorMessage,
        RestStatus restStatus
    ) {
        throw new UnsupportedOperationException(
            "Custom error handling is not implemented. Please override buildProviderSpecificChatCompletionError method."
        );
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
    protected UnifiedChatCompletionException buildDefaultChatCompletionError(
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
     * Builds a mid-stream error for a streaming request.
     * This method is used when an error occurs while processing a streaming response.
     * It must be implemented by subclasses to handle specific error response formats.
     * Only streaming requests should use this method.
     *
     * @param inferenceEntityId the ID of the inference entity
     * @param message           the error message
     * @param e                the exception that caused the error, can be null
     * @return a {@link UnifiedChatCompletionException} representing the mid-stream error
     */
    public UnifiedChatCompletionException buildMidStreamChatCompletionError(String inferenceEntityId, String message, Exception e) {
        throw new UnsupportedOperationException(
            "Mid-stream error handling is not implemented. Please override buildMidStreamChatCompletionError method."
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
     * @param errorResponseClass the class of the expected error response type
     * @return a {@link UnifiedChatCompletionException} representing the mid-stream error
     */
    protected UnifiedChatCompletionException buildMidStreamChatCompletionError(
        String inferenceEntityId,
        String message,
        Exception e,
        Class<? extends ErrorResponse> errorResponseClass
    ) {
        // Extract the error response from the message using the provided method
        var errorResponse = extractMidStreamChatCompletionErrorResponse(message);
        // Check if the error response matches the expected type
        if (errorResponseClass.isInstance(errorResponse)) {
            // If it matches, we can build a custom mid-stream error exception
            return buildProviderSpecificMidStreamChatCompletionError(inferenceEntityId, errorResponse);
        } else if (e != null) {
            // If the error response does not match, we can still return an exception based on the original throwable
            return UnifiedChatCompletionException.fromThrowable(e);
        } else {
            // If no specific error response is found, we return a default mid-stream error
            return buildDefaultMidStreamChatCompletionError(inferenceEntityId, errorResponse);
        }
    }

    /**
     * Builds a custom mid-stream {@link UnifiedChatCompletionException} for a streaming request.
     * This method is called when a specific error response is found in the message.
     * It must be implemented by subclasses to handle specific error response formats.
     * Only streaming requests should use this method.
     *
     * @param inferenceEntityId the ID of the inference entity
     * @param errorResponse     the error response parsed from the message
     * @return an instance of {@link UnifiedChatCompletionException} with details from the error response
     */
    protected UnifiedChatCompletionException buildProviderSpecificMidStreamChatCompletionError(
        String inferenceEntityId,
        ErrorResponse errorResponse
    ) {
        throw new UnsupportedOperationException(
            "Mid-stream error handling is not implemented for this response handler. "
                + "Please override buildProviderSpecificMidStreamChatCompletionError method."
        );
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
    protected UnifiedChatCompletionException buildDefaultMidStreamChatCompletionError(
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
     * Extracts the mid-stream error response from the message.
     * This method is used to parse the error response from a streaming message.
     * It must be implemented by subclasses to handle specific error response formats.
     * Only streaming requests should use this method.
     *
     * @param message the message containing the error response
     * @return an {@link ErrorResponse} object representing the mid-stream error
     */
    protected ErrorResponse extractMidStreamChatCompletionErrorResponse(String message) {
        throw new UnsupportedOperationException(
            "Mid-stream error extraction is not implemented. Please override extractMidStreamChatCompletionErrorResponse method."
        );
    }

    /**
     * Creates a string representation of the error type based on the provided ErrorResponse.
     * This method is used to generate a human-readable error type for logging or exception messages.
     *
     * @param errorResponse the ErrorResponse object
     * @return a string representing the error type
     */
    protected static String createErrorType(ErrorResponse errorResponse) {
        return errorResponse != null ? errorResponse.getClass().getSimpleName() : "unknown";
    }

    protected String errorMessage(String message, Request request, ErrorResponse errorResponse, int statusCode) {
        return (errorResponse == null
            || errorResponse.errorStructureFound() == false
            || Strings.isNullOrEmpty(errorResponse.getErrorMessage()))
                ? format("%s for request from inference entity id [%s] status [%s]", message, request.getInferenceEntityId(), statusCode)
                : format(
                    "%s for request from inference entity id [%s] status [%s]. Error message: [%s]",
                    message,
                    request.getInferenceEntityId(),
                    statusCode,
                    errorResponse.getErrorMessage()
                );
    }

    public static RestStatus toRestStatus(int statusCode) {
        RestStatus code = null;
        if (statusCode < 500) {
            code = RestStatus.fromCode(statusCode);
        }

        return code == null ? RestStatus.BAD_REQUEST : code;
    }
}
