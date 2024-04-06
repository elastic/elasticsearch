/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.azureopenai;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.azureopenai.AzureOpenAiErrorResponseEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;
import static org.elasticsearch.xpack.inference.external.http.retry.ResponseHandlerUtils.getFirstHeaderOrUnknown;

public class AzureOpenAiResponseHandler extends BaseResponseHandler {

    /**
     * These headers for Azure OpenAi are mostly the same as the OpenAi ones with the major exception
     * that there is no information returned about the request limit or the tokens limit
     *
     * Microsoft does not seem to have any information in their docs about this, but more
     * information can be found in the following Medium article and accompanying code:
     *   - https://pablo-81685.medium.com/azure-openais-api-headers-unpacked-6dbe881e732a
     *   - https://github.com/pablosalvador10/gbbai-azure-ai-aoai
     *
     * An example from CURL shows:
     * HTTP/2 200
     * content-length: 33525
     * content-type: application/json
     * access-control-allow-origin: *
     * x-ratelimit-remaining-requests: 119
     * apim-request-id: cb28c608-ff0b-43c0-baca-f6c60a2d120b
     * strict-transport-security: max-age=31536000; includeSubDomains; preload
     * x-content-type-options: nosniff
     * x-request-id: 93117dcb-0193-49cf-9f8e-7aeb2995297d
     * x-ms-client-request-id: cb28c608-ff0b-43c0-baca-f6c60a2d120b
     * x-ratelimit-remaining-tokens: 119990
     * azureml-model-session: d012-20240327190940
     * x-ms-region: East US 2
     * date: Fri, 05 Apr 2024 12:26:05 GMT
     *
     */
    static final String REMAINING_REQUESTS = "x-ratelimit-remaining-requests";
    // The remaining number of tokens that are permitted before exhausting the rate limit.
    static final String REMAINING_TOKENS = "x-ratelimit-remaining-tokens";

    static final String CONTENT_TOO_LARGE_MESSAGE = "Please reduce your prompt; or completion length.";

    static final String AZURE_OPENAI_SERVER_BUSY = "Received a server busy error status code";

    public AzureOpenAiResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, AzureOpenAiErrorResponseEntity::fromResponse);
    }

    @Override
    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, Request request, HttpResult result)
        throws RetryException {
        checkForFailureStatusCode(request, result);
        checkForEmptyBody(throttlerManager, logger, request, result);
    }

    /**
     * Validates the status code throws an RetryException if not in the range [200, 300).
     *
     * The OpenAI API error codes are documented <a href="https://platform.openai.com/docs/guides/error-codes/api-errors">here</a>.
     * @param request The originating request
     * @param result  The http response and body
     * @throws RetryException Throws if status code is {@code >= 300 or < 200 }
     */
    void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode >= 200 && statusCode < 300) {
            return;
        }

        // handle error codes
        if (statusCode == 500) {
            throw new RetryException(true, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 503) {
            throw new RetryException(true, buildError(AZURE_OPENAI_SERVER_BUSY, request, result));
        } else if (statusCode > 500) {
            throw new RetryException(false, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 429) {
            throw buildExceptionHandling429(request, result);
        } else if (isContentTooLarge(result)) {
            throw new ContentTooLargeException(buildError(CONTENT_TOO_LARGE, request, result));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }

    RetryException buildExceptionHandling429(Request request, HttpResult result) {
        return new RetryException(true, buildError(buildRateLimitErrorMessage(result), request, result));
    }

    private static boolean isContentTooLarge(HttpResult result) {
        int statusCode = result.response().getStatusLine().getStatusCode();

        if (statusCode == 413) {
            return true;
        }

        if (statusCode == 400) {
            var errorEntity = AzureOpenAiErrorResponseEntity.fromResponse(result);

            return errorEntity != null && errorEntity.getErrorMessage().contains(CONTENT_TOO_LARGE_MESSAGE);
        }

        return false;
    }

    static String buildRateLimitErrorMessage(HttpResult result) {
        var response = result.response();
        var remainingTokens = getFirstHeaderOrUnknown(response, REMAINING_TOKENS);
        var remainingRequests = getFirstHeaderOrUnknown(response, REMAINING_REQUESTS);
        var usageMessage = Strings.format("Remaining tokens [%s]. Remaining requests [%s].", remainingTokens, remainingRequests);

        return RATE_LIMIT + ". " + usageMessage;
    }

}
