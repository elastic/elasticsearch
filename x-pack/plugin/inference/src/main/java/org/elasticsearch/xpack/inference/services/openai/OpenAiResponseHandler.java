/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiErrorResponseEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;

public class OpenAiResponseHandler implements ResponseHandler {

    protected final String requestType;
    private final CheckedFunction<HttpResult, InferenceServiceResults, IOException> parseFunction;

    public OpenAiResponseHandler(String requestType, CheckedFunction<HttpResult, InferenceServiceResults, IOException> parseFunction) {
        this.requestType = Objects.requireNonNull(requestType);
        this.parseFunction = Objects.requireNonNull(parseFunction);
    }

    @Override
    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, HttpRequestBase request, HttpResult result)
        throws RetryException {
        checkForFailureStatusCode(request, result);
        checkForEmptyBody(throttlerManager, logger, request, result);
    }

    @Override
    public InferenceServiceResults parseResult(HttpResult result) throws RetryException {
        try {
            return parseFunction.apply(result);
        } catch (Exception e) {
            throw new RetryException(true, e);
        }
    }

    @Override
    public String getRequestType() {
        return requestType;
    }

    /**
     * Validates the status code throws an RetryException if not in the range [200, 300).
     *
     * The OpenAI API error codes are document at https://platform.openai.com/docs/guides/error-codes/api-errors
     * @param request The http request
     * @param result  The http response and body
     * @throws RetryException Throws if status code is {@code >= 300 or < 200 }
     */
    static void checkForFailureStatusCode(HttpRequestBase request, HttpResult result) throws RetryException {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode >= 200 && statusCode < 300) {
            return;
        }

        // handle error codes
        if (statusCode >= 500) {
            String errorMsg = buildErrorMessageWithResponse(
                "Received a server error status code for request [%s] status [%s]",
                request.getRequestLine(),
                statusCode,
                result
            );
            throw new RetryException(false, errorMsg);
        } else if (statusCode == 429) {
            String errorMsg = buildErrorMessageWithResponse(
                "Received a rate limit status code for request [%s] status [%s]",
                request.getRequestLine(),
                statusCode,
                result
            );
            throw new RetryException(false, errorMsg); // TODO back off and retry
        } else if (statusCode == 401) {
            String errorMsg = buildErrorMessageWithResponse(
                "Received a authentication error status code for request [%s] status [%s]",
                request.getRequestLine(),
                statusCode,
                result
            );
            throw new RetryException(false, errorMsg);
        } else if (statusCode >= 300 && statusCode < 400) {
            String errorMsg = buildErrorMessageWithResponse(
                "Unhandled redirection for request [%s] status [%s]",
                request.getRequestLine(),
                statusCode,
                result
            );
            throw new RetryException(false, errorMsg);
        } else {
            String errorMsg = buildErrorMessageWithResponse(
                "Received an unsuccessful status code for request [%s] status [%s]",
                request.getRequestLine(),
                statusCode,
                result
            );
            throw new RetryException(false, errorMsg);
        }
    }

    static String buildErrorMessageWithResponse(String baseMessage, RequestLine requestLine, int statusCode, HttpResult response) {
        var errorEntity = OpenAiErrorResponseEntity.fromResponse(response);

        if (errorEntity == null) {
            return format(baseMessage, requestLine, statusCode);
        } else {
            var base = format(baseMessage, requestLine, statusCode);
            return base + ". Error message: [" + errorEntity.getErrorMessage() + "]";
        }

    }
}
