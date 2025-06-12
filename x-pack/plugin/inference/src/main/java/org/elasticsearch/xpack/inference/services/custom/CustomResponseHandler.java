/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.custom.response.ErrorResponseParser;

/**
 * Defines how to handle various response types returned from the custom integration.
 */
public class CustomResponseHandler extends BaseResponseHandler {
    public CustomResponseHandler(String requestType, ResponseParser parseFunction, ErrorResponseParser errorParser) {
        super(requestType, parseFunction, errorParser);
    }

    @Override
    public InferenceServiceResults parseResult(Request request, HttpResult result) throws RetryException {
        try {
            return parseFunction.apply(request, result);
        } catch (Exception e) {
            // if we get a parse failure it's probably an incorrect configuration of the service so report the error back to the user
            // immediately without retrying
            throw new RetryException(
                false,
                new ElasticsearchStatusException(
                    "Failed to parse custom model response, please check that the response parser path matches the response format.",
                    RestStatus.BAD_REQUEST,
                    e
                )
            );
        }
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
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode >= 200 && statusCode < 300) {
            return;
        }

        // handle error codes
        if (statusCode >= 500) {
            throw new RetryException(false, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 429) {
            throw new RetryException(true, buildError(RATE_LIMIT, request, result));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }
}
