/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiErrorResponseEntity;

import java.util.function.Function;

/**
 * Maps OCI Generative AI HTTP error responses to retryable and non-retryable failures.
 *
 * @see <a href="https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm">OCI API errors</a>
 */
public class OciGenAiResponseHandler extends BaseResponseHandler {

    public OciGenAiResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, OciGenAiErrorResponseEntity::fromResponse);
    }

    public OciGenAiResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        Function<HttpResult, ErrorResponse> errorParseFunction,
        boolean canHandleStreamingResponses
    ) {
        super(requestType, parseFunction, errorParseFunction, canHandleStreamingResponses);
    }

    @Override
    public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode >= 500) {
            return new RetryException(true, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode == 429) {
            return new RetryException(true, buildError(RATE_LIMIT, outboundRequest, result));
        } else if (statusCode == 404) {
            return new RetryException(false, buildError(resourceNotFoundError(outboundRequest), outboundRequest, result));
        } else if (statusCode == 403) {
            return new RetryException(false, buildError(PERMISSION_DENIED, outboundRequest, result));
        } else if (statusCode == 401) {
            return new RetryException(false, buildError(AUTHENTICATION, outboundRequest, result));
        } else if (statusCode == 400) {
            return new RetryException(false, buildError(BAD_REQUEST, outboundRequest, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            return new RetryException(false, buildError(REDIRECTION, outboundRequest, result));
        } else {
            return new RetryException(false, buildError(UNSUCCESSFUL, outboundRequest, result));
        }
    }
}
