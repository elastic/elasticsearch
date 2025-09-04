/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;

import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.external.http.retry.ResponseHandlerUtils.getFirstHeaderOrUnknown;

public class AnthropicResponseHandler extends BaseResponseHandler {
    /**
     * Rate limit headers taken from https://docs.anthropic.com/en/api/rate-limits#response-headers
     */
    // The maximum number of requests allowed within the rate limit window.
    static final String REQUESTS_LIMIT = "anthropic-ratelimit-requests-limit";
    // The number of requests remaining within the current rate limit window.
    static final String REMAINING_REQUESTS = "anthropic-ratelimit-requests-remaining";
    // The time when the request rate limit window will reset, provided in RFC 3339 format.
    static final String REQUEST_RESET = "anthropic-ratelimit-requests-reset";
    // The maximum number of tokens allowed within the rate limit window.
    static final String TOKENS_LIMIT = "anthropic-ratelimit-tokens-limit";
    // The number of tokens remaining, rounded to the nearest thousand, within the current rate limit window.
    static final String REMAINING_TOKENS = "anthropic-ratelimit-tokens-remaining";
    // The time when the token rate limit window will reset, provided in RFC 3339 format.
    static final String TOKENS_RESET = "anthropic-ratelimit-tokens-reset";
    // The number of seconds until the rate limit window resets.
    static final String RETRY_AFTER = "retry-after";

    static final String SERVER_BUSY = "Received an Anthropic server is temporarily overloaded status code";

    public AnthropicResponseHandler(String requestType, ResponseParser parseFunction, boolean canHandleStreamingResponses) {
        super(requestType, parseFunction, ErrorMessageResponseEntity::fromResponse, canHandleStreamingResponses);
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var sseProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var anthropicProcessor = new AnthropicStreamingProcessor();
        sseProcessor.subscribe(anthropicProcessor);
        flow.subscribe(sseProcessor);
        return new StreamingChatCompletionResults(anthropicProcessor);
    }

    /**
     * Validates the status code throws an RetryException if not in the range [200, 300).
     *
     * The Anthropic API error codes are documented <a href="https://docs.anthropic.com/en/api/errors">here</a>.
     * @param request The originating request
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
        } else if (statusCode == 529) {
            throw new RetryException(true, buildError(SERVER_BUSY, request, result));
        } else if (statusCode > 500) {
            throw new RetryException(false, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 429) {
            throw new RetryException(true, buildError(buildRateLimitErrorMessage(result), request, result));
        } else if (statusCode == 403) {
            throw new RetryException(false, buildError(PERMISSION_DENIED, request, result));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }

    static String buildRateLimitErrorMessage(HttpResult result) {
        var response = result.response();
        var tokenLimit = getFirstHeaderOrUnknown(response, TOKENS_LIMIT);
        var remainingTokens = getFirstHeaderOrUnknown(response, REMAINING_TOKENS);
        var requestLimit = getFirstHeaderOrUnknown(response, REQUESTS_LIMIT);
        var remainingRequests = getFirstHeaderOrUnknown(response, REMAINING_REQUESTS);
        var requestReset = getFirstHeaderOrUnknown(response, REQUEST_RESET);
        var tokensReset = getFirstHeaderOrUnknown(response, TOKENS_RESET);
        var retryAfter = getFirstHeaderOrUnknown(response, RETRY_AFTER);

        var usageMessage = Strings.format(
            "Token limit [%s], remaining tokens [%s], tokens reset [%s]. "
                + "Request limit [%s], remaining requests [%s], request reset [%s]. Retry after [%s]",
            tokenLimit,
            remainingTokens,
            tokensReset,
            requestLimit,
            remainingRequests,
            requestReset,
            retryAfter
        );

        return RATE_LIMIT + ". " + usageMessage;
    }
}
