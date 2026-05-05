/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.apache.http.Header;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceErrorResponseEntity;

public class ElasticInferenceServiceResponseHandler extends BaseResponseHandler {

    public static final String RETRY_AFTER_HEADER = "Retry-After";

    public ElasticInferenceServiceResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, ElasticInferenceServiceErrorResponseEntity::fromResponse);
    }

    public ElasticInferenceServiceResponseHandler(String requestType, ResponseParser parseFunction, boolean canHandleStreamingResponses) {
        super(requestType, parseFunction, ElasticInferenceServiceErrorResponseEntity::fromResponse, canHandleStreamingResponses);
    }

    @Override
    protected void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        throw buildRetryException(request, result);
    }

    private RetryException buildRetryException(Request request, HttpResult result) {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500 || statusCode == 503) {
            return new RetryException(true, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 400) {
            return new RetryException(false, buildError(BAD_REQUEST, request, result));
        } else if (statusCode == 405) {
            return new RetryException(false, buildError(METHOD_NOT_ALLOWED, request, result));
        } else if (statusCode == 413) {
            return new ContentTooLargeException(buildError(CONTENT_TOO_LARGE, request, result));
        } else if (statusCode == 429) {
            return new RetryException(true, buildError(RATE_LIMIT, request, result));
        }

        return new RetryException(false, buildError(UNSUCCESSFUL, request, result));
    }

    @Override
    protected ElasticsearchException buildError(String message, Request request, HttpResult result) {
        ElasticsearchException error = super.buildError(message, request, result);
        addRetryAfterHeaderIfPresent(result, error);
        return error;
    }

    private void addRetryAfterHeaderIfPresent(HttpResult result, ElasticsearchException e) {
        Header retryAfterHeader = result.response().getFirstHeader(RETRY_AFTER_HEADER);
        if (retryAfterHeader != null) {
            e.addHttpHeader(RETRY_AFTER_HEADER, retryAfterHeader.getValue());
        }
    }
}
