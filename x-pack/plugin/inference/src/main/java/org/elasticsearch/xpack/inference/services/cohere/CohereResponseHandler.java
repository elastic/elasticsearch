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
import org.elasticsearch.xpack.inference.external.request.Request;
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
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var ndProcessor = new NewlineDelimitedByteProcessor();
        var cohereProcessor = new CohereStreamingProcessor();
        flow.subscribe(ndProcessor);
        ndProcessor.subscribe(cohereProcessor);
        return new StreamingChatCompletionResults(cohereProcessor);
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
        } else if (isTextsArrayTooLarge(result)) {
            throw new RetryException(false, buildError(TEXTS_ARRAY_ERROR_MESSAGE, request, result));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }

    private static boolean isTextsArrayTooLarge(HttpResult result) {
        int statusCode = result.response().getStatusLine().getStatusCode();

        if (statusCode == 400) {
            var errorEntity = CohereErrorResponseEntity.fromResponse(result);
            return errorEntity != null && errorEntity.getErrorMessage().contains(TEXTS_ARRAY_TOO_LARGE_MESSAGE_MATCHER);
        }

        return false;
    }
}
