/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

/**
 * Response handler for ContextualAI API calls.
 */
public class ContextualAiResponseHandler extends BaseResponseHandler {

    public ContextualAiResponseHandler(String requestType, ResponseParser parseFunction, boolean supportsStreaming) {
        super(requestType, parseFunction, ErrorResponse::fromResponse, supportsStreaming);
    }

    @Override
    protected void checkForFailureStatusCode(OutboundRequest outboundRequest, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        // handle error codes
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            throw new RetryException(true, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode == 503) {
            throw new RetryException(true, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode > 500) {
            throw new RetryException(false, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode == 429) {
            throw new RetryException(true, buildError(RATE_LIMIT, outboundRequest, result));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, outboundRequest, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, outboundRequest, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, outboundRequest, result));
        }
    }
}
