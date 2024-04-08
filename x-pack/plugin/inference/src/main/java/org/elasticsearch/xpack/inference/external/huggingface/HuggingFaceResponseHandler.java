/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.huggingface;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.huggingface.HuggingFaceErrorResponseEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;

public class HuggingFaceResponseHandler extends BaseResponseHandler {

    public HuggingFaceResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, HuggingFaceErrorResponseEntity::fromResponse);
    }

    @Override
    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, Request request, HttpResult result)
        throws RetryException {
        checkForFailureStatusCode(request, result);
        checkForEmptyBody(throttlerManager, logger, request, result);
    }

    /**
     * Validates the status code and throws a RetryException if it is not in the range [200, 300).
     *
     * The Hugging Face error codes are loosely defined <a href="https://huggingface.co/docs/api-inference/faq">here</a>.
     * @param request the http request
     * @param result the http response and body
     * @throws RetryException thrown if status code is {@code >= 300 or < 200}
     */
    void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode >= 200 && statusCode < 300) {
            return;
        }

        if (statusCode == 503 || statusCode == 502 || statusCode == 429) {
            throw new RetryException(true, buildError(RATE_LIMIT, request, result));
        } else if (statusCode >= 500) {
            throw new RetryException(false, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 413) {
            throw new ContentTooLargeException(buildError(CONTENT_TOO_LARGE, request, result));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }
}
