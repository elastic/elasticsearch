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
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
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
    protected void checkForFailureStatusCode(OutboundRequest outboundRequest, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        throw buildRetryException(outboundRequest, result);
    }

    private RetryException buildRetryException(OutboundRequest outboundRequest, HttpResult result) {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500 || statusCode == 503) {
            throw new RetryException(true, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode == 400) {
            throw new RetryException(false, buildError(BAD_REQUEST, outboundRequest, result));
        } else if (statusCode == 405) {
            throw new RetryException(false, buildError(METHOD_NOT_ALLOWED, outboundRequest, result));
        } else if (statusCode == 413) {
            throw new ContentTooLargeException(buildError(CONTENT_TOO_LARGE, outboundRequest, result));
        } else if (statusCode == 429) {
            throw new RetryException(true, buildError(RATE_LIMIT, outboundRequest, result));
        }

        return new RetryException(false, buildError(UNSUCCESSFUL, outboundRequest, result));
    }

    @Override
    protected ElasticsearchException buildError(String message, OutboundRequest outboundRequest, HttpResult result) {
        ElasticsearchException error = super.buildError(message, outboundRequest, result);
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
