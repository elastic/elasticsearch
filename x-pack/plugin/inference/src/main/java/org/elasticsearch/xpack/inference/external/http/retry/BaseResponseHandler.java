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
import org.elasticsearch.xpack.inference.external.request.Request;
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

    protected void checkForErrorObject(Request request, HttpResult result) {
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
        var errorEntityMsg = errorParseFunction.apply(result);
        return buildError(message, request, result, errorEntityMsg);
    }

    protected Exception buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        var responseStatusCode = result.response().getStatusLine().getStatusCode();
        return new ElasticsearchStatusException(
            constructErrorMessage(message, request, errorResponse, responseStatusCode),
            toRestStatus(responseStatusCode)
        );
    }

    public static String constructErrorMessage(String message, Request request, ErrorResponse errorResponse, int statusCode) {
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
