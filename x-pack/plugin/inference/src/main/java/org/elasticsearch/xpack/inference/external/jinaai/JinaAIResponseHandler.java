/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.jinaai;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.jinaai.JinaAIErrorResponseEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;

/**
 * Defines how to handle various errors returned from the JinaAI integration.
 *
 */
public class JinaAIResponseHandler extends BaseResponseHandler {
    static final String INPUTS_ARRAY_TOO_LARGE_MESSAGE_MATCHER = "is larger than the largest allowed size";
    static final String INPUTS_ARRAY_ERROR_MESSAGE = "Received an inputs array too large response";
    static final String PAYMENT_ERROR_MESSAGE = "Payment required";

    public JinaAIResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, JinaAIErrorResponseEntity::fromResponse);
    }

    @Override
    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, Request request, HttpResult result)
        throws RetryException {
        checkForFailureStatusCode(request, result);
        checkForEmptyBody(throttlerManager, logger, request, result);
    }

    @Override
    public boolean canHandleStreamingResponses() {
        // TODO (JoanFM): Check this usage
        return false;
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        // TODO (JoanFM): Check this logic
        return new StreamingChatCompletionResults(null);
    }

    /**
     * Validates the status code throws an RetryException if not in the range [200, 300).
     *
     * @param request The http request
     * @param result  The http response and body
     * @throws RetryException Throws if status code is {@code >= 300 or < 200 }
     */
    void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
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
        } else if (statusCode == 400 && isTextsArrayTooLarge(result, statusCode)) {
            throw new RetryException(false, buildError(INPUTS_ARRAY_ERROR_MESSAGE, request, result));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode == 402) {
            throw new RetryException(false, buildError(PAYMENT_ERROR_MESSAGE, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }

    private static boolean isTextsArrayTooLarge(HttpResult result, int statusCode) {

        if (statusCode == 400) {
            var errorEntity = JinaAIErrorResponseEntity.fromResponse(result);

            return errorEntity != null && errorEntity.getErrorMessage().contains(INPUTS_ARRAY_TOO_LARGE_MESSAGE_MATCHER);
        }

        return false;
    }
}
