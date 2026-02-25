/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;

/**
 * Response handler for FireworksAI API calls.
 * Handles HTTP status codes and determines retry behavior for failed requests.
 * FireworksAI uses OpenAI-compatible error response format.
 */
public class FireworksAiResponseHandler extends BaseResponseHandler {

    public FireworksAiResponseHandler(String requestType, ResponseParser parseFunction, boolean supportsStreaming) {
        super(requestType, parseFunction, ErrorMessageResponseEntity::fromResponse, supportsStreaming);
    }

    @Override
    protected void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        // Handle error status codes
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            // 500 errors are retryable (temporary server issues)
            throw new RetryException(true, buildError(SERVER_ERROR, request, result));
        } else if (statusCode > 500) {
            // 501+ errors are generally not retryable
            throw new RetryException(false, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 429) {
            // Rate limit - retryable after backoff
            throw new RetryException(true, buildError(RATE_LIMIT, request, result));
        } else if (statusCode == 401) {
            // Authentication error - not retryable
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            // Redirection - not retryable
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            // Other 4xx errors - not retryable
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }
}
