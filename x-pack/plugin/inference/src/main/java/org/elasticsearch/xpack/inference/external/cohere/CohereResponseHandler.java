/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.cohere;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.response.cohere.CohereErrorResponseEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;
import static org.elasticsearch.xpack.inference.external.http.retry.ResponseHandlerUtils.getFirstHeaderOrUnknown;

public class CohereResponseHandler extends BaseResponseHandler {

    static final String MONTHLY_REQUESTS_LIMIT = "x-endpoint-monthly-call-limit";
    // TODO determine the production versions of these
    static final String TRIAL_REQUEST_LIMIT_PER_MINUTE = "x-trial-endpoint-call-limit";
    static final String TRIAL_REQUESTS_REMAINING = "x-trial-endpoint-call-remaining";
    static final String TEXTS_ARRAY_TOO_LARGE_MESSAGE_MATCHER = "invalid request: total number of texts must be at most";
    static final String TEXTS_ARRAY_ERROR_MESSAGE = "Received a texts array too large response";

    public CohereResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, CohereErrorResponseEntity::fromResponse);
    }

    @Override
    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, HttpRequestBase request, HttpResult result)
        throws RetryException {
        checkForFailureStatusCode(request, result);
        checkForEmptyBody(throttlerManager, logger, request, result);
    }

    /**
     * Validates the status code throws an RetryException if not in the range [200, 300).
     *
     * The OpenAI API error codes are documented <a href="https://platform.openai.com/docs/guides/error-codes/api-errors">here</a>.
     * @param request The http request
     * @param result  The http response and body
     * @throws RetryException Throws if status code is {@code >= 300 or < 200 }
     */
    void checkForFailureStatusCode(HttpRequestBase request, HttpResult result) throws RetryException {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode >= 200 && statusCode < 300) {
            return;
        }

        // handle error codes
        if (statusCode >= 500) {
            throw new RetryException(false, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 429) {
            throw new RetryException(true, buildError(buildRateLimitErrorMessage(result), request, result));
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

    static String buildRateLimitErrorMessage(HttpResult result) {
        var response = result.response();
        var monthlyRequestLimit = getFirstHeaderOrUnknown(response, MONTHLY_REQUESTS_LIMIT);
        var trialRequestsPerMinute = getFirstHeaderOrUnknown(response, TRIAL_REQUEST_LIMIT_PER_MINUTE);
        var trialRequestsRemaining = getFirstHeaderOrUnknown(response, TRIAL_REQUESTS_REMAINING);

        var usageMessage = Strings.format(
            "Monthly request limit [%s], permitted requests per minute [%s], remaining requests [%s]",
            monthlyRequestLimit,
            trialRequestsPerMinute,
            trialRequestsRemaining
        );

        return RATE_LIMIT + ". " + usageMessage;
    }
}
