/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.response;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.openai.OpenAiStreamingProcessor;

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
        Function<HttpResult, ErrorResponse> errorParseFunction,
        boolean canHandleStreamingResponses
    ) {
        super(requestType, parseFunction, errorParseFunction);
        this.canHandleStreamingResponses = canHandleStreamingResponses;
    }

    @Override
    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, OutboundRequest outboundRequest, HttpResult result)
        throws RetryException {
        checkForFailureStatusCode(outboundRequest, result);
        checkForEmptyBody(throttlerManager, logger, outboundRequest, result);
    }

    @Override
    public boolean canHandleStreamingResponses() {
        return canHandleStreamingResponses;
    }

    @Override
    public InferenceServiceResults parseResult(OutboundRequest outboundRequest, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var openAiProcessor = new OpenAiStreamingProcessor();

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(openAiProcessor);
        return new StreamingChatCompletionResults(openAiProcessor);
    }

    public void checkForFailureStatusCode(OutboundRequest outboundRequest, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        // handle error codes
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            throw handle500Error(outboundRequest, result);
        } else if (statusCode == 503) {
            throw handle503Error(outboundRequest, result);
        } else if (statusCode > 500) {
            throw handleOther500Error(outboundRequest, result);
        } else if (statusCode == 429) {
            throw handleRateLimitingError(outboundRequest, result);
        } else if (isContentTooLarge(result)) {
            throw new ContentTooLargeException(buildError(CONTENT_TOO_LARGE, outboundRequest, result));
        } else if (statusCode == 401) {
            throw handleAuthenticationError(outboundRequest, result);
        } else if (statusCode >= 300 && statusCode < 400) {
            throw handleRedirectionStatusCode(outboundRequest, result);
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, outboundRequest, result));
        }
    }

    protected RetryException handle500Error(OutboundRequest outboundRequest, HttpResult result) {
        return new RetryException(true, buildError(SERVER_ERROR, outboundRequest, result));
    }

    protected RetryException handle503Error(OutboundRequest outboundRequest, HttpResult result) {
        return new RetryException(true, buildError(SERVER_BUSY_ERROR, outboundRequest, result));
    }

    protected RetryException handleOther500Error(OutboundRequest outboundRequest, HttpResult result) {
        return new RetryException(false, buildError(SERVER_ERROR, outboundRequest, result));
    }

    protected RetryException handleAuthenticationError(OutboundRequest outboundRequest, HttpResult result) {
        return new RetryException(false, buildError(AUTHENTICATION, outboundRequest, result));
    }

    protected RetryException handleRateLimitingError(OutboundRequest outboundRequest, HttpResult result) {
        return new RetryException(true, buildError(buildRateLimitErrorMessage(result), outboundRequest, result));
    }

    protected RetryException handleRedirectionStatusCode(OutboundRequest outboundRequest, HttpResult result) {
        throw new RetryException(false, buildError(REDIRECTION, outboundRequest, result));
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
