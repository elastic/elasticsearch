/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

public abstract class BaseResponseHandler implements ResponseHandler {

    public static final String SERVER_ERROR = "Received a server error status code";
    public static final String RATE_LIMIT = "Received a rate limit status code";
    public static final String AUTHENTICATION = "Received an authentication error status code";
    public static final String REDIRECTION = "Unhandled redirection";
    public static final String CONTENT_TOO_LARGE = "Received a content too large status code";
    public static final String UNSUCCESSFUL = "Received an unsuccessful status code";

    protected final String requestType;
    private final ResponseParser parseFunction;
    private final Function<HttpResult, ErrorMessage> errorParseFunction;

    public BaseResponseHandler(String requestType, ResponseParser parseFunction, Function<HttpResult, ErrorMessage> errorParseFunction) {
        this.requestType = Objects.requireNonNull(requestType);
        this.parseFunction = Objects.requireNonNull(parseFunction);
        this.errorParseFunction = Objects.requireNonNull(errorParseFunction);
    }

    @Override
    public InferenceServiceResults parseResult(Request request, HttpResult result) throws RetryException {
        try {
            return parseFunction.apply(request, result);
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
        var responseStatusCode = result.response().getStatusLine().getStatusCode();

        if (errorEntityMsg == null) {
            return new ElasticsearchStatusException(
                format("%s for request [%s] status [%s]", message, request.getRequestLine(), responseStatusCode),
                toRestStatus(responseStatusCode)
            );
        }

        return new ElasticsearchStatusException(
            format(
                "%s for request [%s] status [%s]. Error message: [%s]",
                message,
                request.getRequestLine(),
                responseStatusCode,
                errorEntityMsg.getErrorMessage()
            ),
            toRestStatus(responseStatusCode)
        );
    }

    public static RestStatus toRestStatus(int statusCode) {
        RestStatus code = null;
        if (statusCode < 500) {
            code = RestStatus.fromCode(statusCode);
        }

        return code == null ? RestStatus.BAD_REQUEST : code;
    }
}
