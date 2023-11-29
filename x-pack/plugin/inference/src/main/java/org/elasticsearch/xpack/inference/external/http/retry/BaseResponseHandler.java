/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

public abstract class BaseResponseHandler implements ResponseHandler {

    public static final String SERVER_ERROR = "Received a server error status code";
    public static final String RATE_LIMIT = "Received a rate limit status code";
    public static final String AUTHENTICATION = "Received an authentication error status code";
    public static final String REDIRECTION = "Unhandled redirection";
    public static final String UNSUCCESSFUL = "Received an unsuccessful status code";

    protected final String requestType;
    private final CheckedFunction<HttpResult, InferenceServiceResults, IOException> parseFunction;
    private final Function<HttpResult, ErrorMessage> errorParseFunction;

    public BaseResponseHandler(
        String requestType,
        CheckedFunction<HttpResult, InferenceServiceResults, IOException> parseFunction,
        Function<HttpResult, ErrorMessage> errorParseFunction
    ) {
        this.requestType = Objects.requireNonNull(requestType);
        this.parseFunction = Objects.requireNonNull(parseFunction);
        this.errorParseFunction = Objects.requireNonNull(errorParseFunction);
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

    protected Exception buildError(String message, HttpRequestBase request, HttpResult result) {
        var errorEntityMsg = errorParseFunction.apply(result);

        if (errorEntityMsg == null) {
            return new ElasticsearchStatusException(
                format(
                    "%s for request [%s] status [%s]",
                    message,
                    request.getRequestLine(),
                    result.response().getStatusLine().getStatusCode()
                ),
                // TODO: should we always return a 502 to the client since we're technically a gateway?
                RestStatus.BAD_GATEWAY
            );
        }

        return new ElasticsearchStatusException(
            format(
                "%s for request [%s] status [%s]. Error message: [%s]",
                message,
                request.getRequestLine(),
                result.response().getStatusLine().getStatusCode(),
                errorEntityMsg.getErrorMessage()
            ),
            RestStatus.BAD_GATEWAY
        );
    }
}
