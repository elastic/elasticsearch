/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorMessage;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.openai.OpenAiStreamingProcessor;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.util.concurrent.Flow;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;
import static org.elasticsearch.xpack.inference.external.http.retry.ResponseHandlerUtils.getFirstHeaderOrUnknown;

/**
 * A base class to use for external response handling.
 * <p>
 * This currently covers response handling for Azure AI Studio, however this pattern
 * can be used to simplify and refactor handling for Azure OpenAI and OpenAI responses.
 */
public class AzureMistralOpenAiExternalResponseHandler extends BaseResponseHandler {

    // The maximum number of requests that are permitted before exhausting the rate limit.
    static final String REQUESTS_LIMIT = "x-ratelimit-limit-requests";
    // The maximum number of tokens that are permitted before exhausting the rate limit.
    static final String TOKENS_LIMIT = "x-ratelimit-limit-tokens";
    // The remaining number of requests that are permitted before exhausting the rate limit.
    static final String REMAINING_REQUESTS = "x-ratelimit-remaining-requests";
    // The remaining number of tokens that are permitted before exhausting the rate limit.
    static final String REMAINING_TOKENS = "x-ratelimit-remaining-tokens";

    static final String CONTENT_TOO_LARGE_MESSAGE = "Please reduce your prompt; or completion length.";
    static final String SERVER_BUSY_ERROR = "Received a server busy error status code";

    private final boolean canHandleStreamingResponses;

    public AzureMistralOpenAiExternalResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        Function<HttpResult, ErrorMessage> errorParseFunction,
        boolean canHandleStreamingResponses
    ) {
        super(requestType, parseFunction, errorParseFunction);
        this.canHandleStreamingResponses = canHandleStreamingResponses;
    }

    @Override
    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, Request request, HttpResult result)
        throws RetryException {
        checkForFailureStatusCode(request, result);
        checkForEmptyBody(throttlerManager, logger, request, result);
    }

    @Override
    public boolean canHandleStreamingResponses() {
        return canHandleStreamingResponses;
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var openAiProcessor = new OpenAiStreamingProcessor();

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(openAiProcessor);
        return new StreamingChatCompletionResults(openAiProcessor);
    }

    public void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        // handle error codes
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            throw handle500Error(request, result);
        } else if (statusCode == 503) {
            throw handle503Error(request, result);
        } else if (statusCode > 500) {
            throw handleOther500Error(request, result);
        } else if (statusCode == 429) {
            throw handleRateLimitingError(request, result);
        } else if (isContentTooLarge(result)) {
            throw new ContentTooLargeException(buildError(CONTENT_TOO_LARGE, request, result));
        } else if (statusCode == 401) {
            throw handleAuthenticationError(request, result);
        } else if (statusCode >= 300 && statusCode < 400) {
            throw handleRedirectionStatusCode(request, result);
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }

    protected RetryException handle500Error(Request request, HttpResult result) {
        return new RetryException(true, buildError(SERVER_ERROR, request, result));
    }

    protected RetryException handle503Error(Request request, HttpResult result) {
        return new RetryException(true, buildError(SERVER_BUSY_ERROR, request, result));
    }

    protected RetryException handleOther500Error(Request request, HttpResult result) {
        return new RetryException(false, buildError(SERVER_ERROR, request, result));
    }

    protected RetryException handleAuthenticationError(Request request, HttpResult result) {
        return new RetryException(false, buildError(AUTHENTICATION, request, result));
    }

    protected RetryException handleRateLimitingError(Request request, HttpResult result) {
        return new RetryException(true, buildError(buildRateLimitErrorMessage(result), request, result));
    }

    protected RetryException handleRedirectionStatusCode(Request request, HttpResult result) {
        throw new RetryException(false, buildError(REDIRECTION, request, result));
    }

    public static boolean isContentTooLarge(HttpResult result) {
        int statusCode = result.response().getStatusLine().getStatusCode();

        if (statusCode == 413) {
            return true;
        }

        if (statusCode == 400) {
            var errorEntity = ErrorMessageResponseEntity.fromResponse(result);
            return errorEntity != null && errorEntity.getErrorMessage().contains(CONTENT_TOO_LARGE_MESSAGE);
        }

        return false;
    }

    public static String buildRateLimitErrorMessage(HttpResult result) {
        var response = result.response();
        var tokenLimit = getFirstHeaderOrUnknown(response, TOKENS_LIMIT);
        var remainingTokens = getFirstHeaderOrUnknown(response, REMAINING_TOKENS);
        var requestLimit = getFirstHeaderOrUnknown(response, REQUESTS_LIMIT);
        var remainingRequests = getFirstHeaderOrUnknown(response, REMAINING_REQUESTS);

        if (tokenLimit.equals("unknown") && requestLimit.equals("unknown")) {
            var usageMessage = Strings.format("Remaining tokens [%s]. Remaining requests [%s].", remainingTokens, remainingRequests);
            return RATE_LIMIT + ". " + usageMessage;
        }

        var usageMessage = Strings.format(
            "Token limit [%s], remaining tokens [%s]. Request limit [%s], remaining requests [%s]",
            tokenLimit,
            remainingTokens,
            requestLimit,
            remainingRequests
        );

        return RATE_LIMIT + ". " + usageMessage;
    }

}
