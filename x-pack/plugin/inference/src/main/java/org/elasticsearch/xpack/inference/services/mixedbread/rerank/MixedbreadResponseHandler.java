/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;

public class MixedbreadResponseHandler extends BaseResponseHandler {
    private static final String FORBIDDEN = "Valid credentials but insufficient permissions for this resource.";
    private static final String PAYMENT_ERROR_MESSAGE = "Insufficient balance. Top up your account to continue.";
    private static final String SERVICE_UNAVAILABLE = "Service temporarily down for maintenance or overloaded. Retry later.";
    private static final String UNPROCESSABLE_ENTITY = "Request format is correct but cannot be processed.";

    public MixedbreadResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, ErrorResponse::fromResponse);
    }

    /**
     * Validates the status code throws an RetryException if not in the range [200, 300).
     *
     * @param request The http request
     * @param result  The http response and body
     * @throws RetryException Throws if status code is {@code >= 400 }
     */
    @Override
    protected void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        // handle error codes
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            throw new RetryException(true, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 503) {
            throw new RetryException(true, buildError(SERVICE_UNAVAILABLE, request, result));
        } else if (statusCode == 429) {
            throw new RetryException(true, buildError(RATE_LIMIT, request, result));
        } else if (statusCode == 422) {
            throw new RetryException(true, buildError(UNPROCESSABLE_ENTITY, request, result));
        } else if (statusCode == 404) {
            throw new RetryException(false, buildError(resourceNotFoundError(request), request, result));
        } else if (statusCode == 403) {
            throw new RetryException(false, buildError(FORBIDDEN, request, result));
        } else if (statusCode == 402) {
            throw new RetryException(false, buildError(PAYMENT_ERROR_MESSAGE, request, result));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode == 400) {
            throw new RetryException(false, buildError(BAD_REQUEST, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }
}
