/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.tencentcloud.response.TencentCloudErrorResponseEntity;

/**
 * Standard response handler for TencentCloud AI Gateway (embeddings and rerank).
 * Chat-completion uses the OpenAI unified handler because the streaming SSE format is OpenAI compatible.
 */
public class TencentCloudResponseHandler extends BaseResponseHandler {

    static final String VALIDATION_ERROR_MESSAGE = "Received an input validation error response";
    static final String PERMISSION_ERROR_MESSAGE = "Permission denied";

    public TencentCloudResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, TencentCloudErrorResponseEntity::fromResponse);
    }

    @Override
    public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            return new RetryException(true, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode > 500) {
            return new RetryException(false, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode == 429) {
            return new RetryException(true, buildError(RATE_LIMIT, outboundRequest, result));
        } else if (statusCode == 400 || statusCode == 422) {
            return new RetryException(false, buildError(VALIDATION_ERROR_MESSAGE, outboundRequest, result));
        } else if (statusCode == 401) {
            return new RetryException(false, buildError(AUTHENTICATION, outboundRequest, result));
        } else if (statusCode == 403) {
            return new RetryException(false, buildError(PERMISSION_ERROR_MESSAGE, outboundRequest, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            return new RetryException(false, buildError(REDIRECTION, outboundRequest, result));
        } else {
            return new RetryException(false, buildError(UNSUCCESSFUL, outboundRequest, result));
        }
    }
}
