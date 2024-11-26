/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.elastic;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceErrorResponseEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;

public class ElasticInferenceServiceResponseHandler extends BaseResponseHandler {

    public ElasticInferenceServiceResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, ElasticInferenceServiceErrorResponseEntity::fromResponse);
    }

    @Override
    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, Request request, HttpResult result)
        throws RetryException {
        checkForFailureStatusCode(request, result);
        checkForEmptyBody(throttlerManager, logger, request, result);
    }

    void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            throw new RetryException(true, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 400) {
            throw new RetryException(false, buildError(BAD_REQUEST, request, result));
        } else if (statusCode == 405) {
            throw new RetryException(false, buildError(METHOD_NOT_ALLOWED, request, result));
        } else if (statusCode == 413) {
            throw new ContentTooLargeException(buildError(CONTENT_TOO_LARGE, request, result));
        }

        throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
    }

}
