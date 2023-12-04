/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiErrorResponseEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;

public class OpenAiResponseHandler extends BaseResponseHandler {

    public OpenAiResponseHandler(String requestType, CheckedFunction<HttpResult, InferenceServiceResults, IOException> parseFunction) {
        super(requestType, parseFunction, OpenAiErrorResponseEntity::fromResponse);
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
            throw new RetryException(false, buildError(RATE_LIMIT, request, result)); // TODO back off and retry
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }
}
