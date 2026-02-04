/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.jinaai.response.JinaAIErrorResponseEntity;

/**
 * Defines how to handle various errors returned from the JinaAI integration.
 *
 */
public class JinaAIResponseHandler extends BaseResponseHandler {
    static final String VALIDATION_ERROR_MESSAGE = "Received an input validation error response";
    static final String PAYMENT_ERROR_MESSAGE = "Payment required";

    public JinaAIResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, JinaAIErrorResponseEntity::fromResponse);
    }

    /**
     * Validates the status code throws an RetryException if not in the range [200, 300).
     *
     * @param request The http request
     * @param result  The http response and body
     * @throws RetryException Throws if status code is {@code >= 300 or < 200 }
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
        } else if (statusCode > 500) {
            throw new RetryException(false, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 429) {
            throw new RetryException(true, buildError(RATE_LIMIT, request, result));
        } else if (statusCode == 400 || statusCode == 422) {
            throw new RetryException(false, buildError(VALIDATION_ERROR_MESSAGE, request, result));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode == 402) {
            throw new RetryException(false, buildError(PAYMENT_ERROR_MESSAGE, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }
}
