/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceErrorResponseEntity;

public class HuggingFaceResponseHandler extends BaseResponseHandler {

    public HuggingFaceResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, HuggingFaceErrorResponseEntity::fromResponse);
    }

    /**
     * Handles failure status codes by returning a RetryException.
     * Only called when the HTTP response status code is not in the range [200, 300).
     *
     * The Hugging Face error codes are loosely defined <a href="https://huggingface.co/docs/api-inference/faq">here</a>.
     * @param outboundRequest the http request
     * @param result the http response and body
     * @return a RetryException describing the failure
     */
    @Override
    public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
        int statusCode = result.response().getCode();
        if (statusCode == 503 || statusCode == 502 || statusCode == 429) {
            return new RetryException(true, buildError(RATE_LIMIT, outboundRequest, result));
        } else if (statusCode >= 500) {
            return new RetryException(false, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode == 413) {
            return new ContentTooLargeException(buildError(CONTENT_TOO_LARGE, outboundRequest, result));
        } else if (statusCode == 401) {
            return new RetryException(false, buildError(AUTHENTICATION, outboundRequest, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            return new RetryException(false, buildError(REDIRECTION, outboundRequest, result));
        } else {
            return new RetryException(false, buildError(UNSUCCESSFUL, outboundRequest, result));
        }
    }
}
