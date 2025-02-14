/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.azureopenai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.openai.OpenAiResponseHandler;
import org.elasticsearch.xpack.inference.external.request.Request;

import static org.elasticsearch.xpack.inference.external.http.retry.ResponseHandlerUtils.getFirstHeaderOrUnknown;

public class AzureOpenAiResponseHandler extends OpenAiResponseHandler {

    /**
     * These headers for Azure OpenAi are mostly the same as the OpenAi ones with the major exception
     * that there is no information returned about the request limit or the tokens limit
     *
     * Microsoft does not seem to have any published information in their docs about this, but more
     * information can be found in the following Medium article and accompanying code:
     *   - https://pablo-81685.medium.com/azure-openais-api-headers-unpacked-6dbe881e732a
     *   - https://github.com/pablosalvador10/gbbai-azure-ai-aoai
     */
    static final String REMAINING_REQUESTS = "x-ratelimit-remaining-requests";
    // The remaining number of tokens that are permitted before exhausting the rate limit.
    static final String REMAINING_TOKENS = "x-ratelimit-remaining-tokens";

    public AzureOpenAiResponseHandler(String requestType, ResponseParser parseFunction, boolean canHandleStreamingResponses) {
        super(requestType, parseFunction, canHandleStreamingResponses);
    }

    @Override
    protected RetryException buildExceptionHandling429(Request request, HttpResult result) {
        return new RetryException(true, buildError(buildRateLimitErrorMessage(result), request, result));
    }

    static String buildRateLimitErrorMessage(HttpResult result) {
        var response = result.response();
        var remainingTokens = getFirstHeaderOrUnknown(response, REMAINING_TOKENS);
        var remainingRequests = getFirstHeaderOrUnknown(response, REMAINING_REQUESTS);
        var usageMessage = Strings.format("Remaining tokens [%s]. Remaining requests [%s].", remainingTokens, remainingRequests);

        return RATE_LIMIT + ". " + usageMessage;
    }
}
