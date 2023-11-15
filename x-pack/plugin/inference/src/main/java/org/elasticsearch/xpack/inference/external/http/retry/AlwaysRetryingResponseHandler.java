/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;
import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForFailureStatusCode;

/**
 * Provides a {@link ResponseHandler} which flags all errors as retryable.
 */
public class AlwaysRetryingResponseHandler implements ResponseHandler {
    protected final String requestType;
    private final CheckedFunction<HttpResult, List<? extends InferenceResults>, IOException> parseFunction;

    public AlwaysRetryingResponseHandler(
        String requestType,
        CheckedFunction<HttpResult, List<? extends InferenceResults>, IOException> parseFunction
    ) {
        this.requestType = Objects.requireNonNull(requestType);
        this.parseFunction = Objects.requireNonNull(parseFunction);
    }

    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, HttpRequestBase request, HttpResult result)
        throws RetryException {
        try {
            checkForFailureStatusCode(throttlerManager, logger, request, result);
            checkForEmptyBody(throttlerManager, logger, request, result);
        } catch (Exception e) {
            throw new RetryException(true, e);
        }
    }

    public String getRequestType() {
        return requestType;
    }

    @Override
    public List<? extends InferenceResults> parseResult(HttpResult result) throws RetryException {
        try {
            return parseFunction.apply(result);
        } catch (Exception e) {
            throw new RetryException(true, e);
        }
    }

}
