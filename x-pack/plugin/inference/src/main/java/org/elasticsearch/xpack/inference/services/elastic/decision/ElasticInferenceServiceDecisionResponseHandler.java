/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.decision;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandler;

public class ElasticInferenceServiceDecisionResponseHandler extends ElasticInferenceServiceResponseHandler {

    private static final int RATE_LIMIT_STATUS_CODE = 429;

    public ElasticInferenceServiceDecisionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction);
    }

    @Override
    public InferenceServiceResults parseResult(OutboundRequest outboundRequest, HttpResult result) throws RetryException {
        try {
            return nonStreamingResponseParseFunction.apply(outboundRequest, result);
        } catch (Exception e) {
            throw new RetryException(false, e);
        }
    }

    @Override
    public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
        var exception = super.buildFailureStatusCodeException(outboundRequest, result);
        if (result.response().getCode() == RATE_LIMIT_STATUS_CODE) {
            return exception;
        }
        if (exception.shouldRetry() == false && exception.getClass() == RetryException.class) {
            return exception;
        }
        // The super class always wraps the actual error, which is what should be returned to the user
        return new RetryException(false, exception.getCause() != null ? exception.getCause() : exception);
    }
}
