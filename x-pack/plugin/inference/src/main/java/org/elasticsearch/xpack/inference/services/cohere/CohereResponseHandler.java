/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.response.streaming.NewlineDelimitedByteProcessor;
import org.elasticsearch.xpack.inference.services.cohere.response.CohereErrorResponseEntity;

import java.util.concurrent.Flow;

/**
 * Defines how to handle various errors returned from the Cohere integration.
 *
 * NOTE:
 * These headers are returned for trial API keys only (they also do not exist within 429 responses)
 *
 * <code>
 * x-endpoint-monthly-call-limit
 * x-trial-endpoint-call-limit
 * x-trial-endpoint-call-remaining
 * </code>
 */
public class CohereResponseHandler extends BaseResponseHandler {
    static final String TEXTS_ARRAY_TOO_LARGE_MESSAGE_MATCHER = "invalid request: total number of texts must be at most";
    static final String TEXTS_ARRAY_ERROR_MESSAGE = "Received a texts array too large response";

    public CohereResponseHandler(String requestType, ResponseParser parseFunction, boolean canHandleStreamingResponse) {
        super(requestType, parseFunction, CohereErrorResponseEntity::fromResponse, canHandleStreamingResponse);
    }

    @Override
    public InferenceServiceResults parseResult(OutboundRequest outboundRequest, Flow.Publisher<HttpResult> flow) {
        var ndProcessor = new NewlineDelimitedByteProcessor();
        var cohereProcessor = new CohereStreamingProcessor();
        flow.subscribe(ndProcessor);
        ndProcessor.subscribe(cohereProcessor);
        return new StreamingChatCompletionResults(cohereProcessor);
    }

    /**
     * Handles failure status codes by returning a RetryException.
     * Only called when the HTTP response status code is not in the range [200, 300).
     *
     * @param outboundRequest The http request
     * @param result  The http response and body
     * @return a RetryException describing the failure
     */
    @Override
    public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
        // handle error codes
        int statusCode = result.response().getCode();
        if (statusCode == 500) {
            return new RetryException(true, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode > 500) {
            return new RetryException(false, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode == 429) {
            return new RetryException(true, buildError(RATE_LIMIT, outboundRequest, result));
        } else if (isTextsArrayTooLarge(result)) {
            return new RetryException(false, buildError(TEXTS_ARRAY_ERROR_MESSAGE, outboundRequest, result));
        } else if (statusCode == 401) {
            return new RetryException(false, buildError(AUTHENTICATION, outboundRequest, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            return new RetryException(false, buildError(REDIRECTION, outboundRequest, result));
        } else {
            return new RetryException(false, buildError(UNSUCCESSFUL, outboundRequest, result));
        }
    }

    private static boolean isTextsArrayTooLarge(HttpResult result) {
        int statusCode = result.response().getCode();

        if (statusCode == 400) {
            var errorEntity = CohereErrorResponseEntity.fromResponse(result);
            return errorEntity != null && errorEntity.getErrorMessage().contains(TEXTS_ARRAY_TOO_LARGE_MESSAGE_MATCHER);
        }

        return false;
    }
}
