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
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

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

    protected final String requestType;
    protected final ResponseParser parseFunction;
    private final Function<HttpResult, ErrorResponse> errorParseFunction;
    private final boolean canHandleStreamingResponses;

    public BaseResponseHandler(String requestType, ResponseParser parseFunction, Function<HttpResult, ErrorResponse> errorParseFunction) {
        this(requestType, parseFunction, errorParseFunction, false);
    }

    public BaseResponseHandler(
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
    public InferenceServiceResults parseResult(OutboundRequest outboundRequest, HttpResult result) throws RetryException {
        try {
            return parseFunction.apply(outboundRequest, result);
        } catch (Exception e) {
            throw new RetryException(true, e);
        }
    }

    @Override
    public String getRequestType() {
        return requestType;
    }

    @Override
    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, OutboundRequest outboundRequest, HttpResult result) {
        checkForFailureStatusCode(outboundRequest, result);
        checkForEmptyBody(throttlerManager, logger, outboundRequest, result);
    }

    protected abstract void checkForFailureStatusCode(OutboundRequest outboundRequest, HttpResult result);

    protected Exception buildError(String message, OutboundRequest outboundRequest, HttpResult result) {
        var errorEntityMsg = errorParseFunction.apply(result);
        return buildError(message, outboundRequest, result, errorEntityMsg);
    }

    protected Exception buildError(String message, OutboundRequest outboundRequest, HttpResult result, ErrorResponse errorResponse) {
        var responseStatusCode = result.response().getStatusLine().getStatusCode();
        return new ElasticsearchStatusException(
            constructErrorMessage(message, outboundRequest, errorResponse, responseStatusCode),
            toRestStatus(responseStatusCode)
        );
    }

    public static String constructErrorMessage(
        String message,
        OutboundRequest outboundRequest,
        ErrorResponse errorResponse,
        int statusCode
    ) {
        return (errorResponse == null
            || errorResponse.errorStructureFound() == false
            || Strings.isNullOrEmpty(errorResponse.getErrorMessage()))
                ? format(
                    "%s for request from inference entity id [%s] status [%s]",
                    message,
                    outboundRequest.getInferenceEntityId(),
                    statusCode
                )
                : format(
                    "%s for request from inference entity id [%s] status [%s]. Error message: [%s]",
                    message,
                    outboundRequest.getInferenceEntityId(),
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

    protected static String resourceNotFoundError(OutboundRequest outboundRequest) {
        return format("Resource not found at [%s]", outboundRequest.getURI());
    }
}
