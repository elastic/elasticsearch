/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.util.Map;
import java.util.Set;

/**
 * Response handler for TencentCloud {@code /v1/multimodal-embeddings}. The API reports parameter values it cannot accept (too many
 * inputs, unsupported image types, oversized responses) as HTTP 500, so a 500 is only retried when the {@code error.code} does not
 * mark it as a request error. Upstream failures (502) are retried.
 */
public class TencentCloudMultimodalResponseHandler extends TencentCloudResponseHandler {

    static final String INVALID_PARAMETER_VALUE = "InvalidParameterValue";
    static final String REQUEST_LIMIT_EXCEEDED = "RequestLimitExceeded";

    private static final Set<String> NON_RETRYABLE_SERVER_ERROR_CODES = Set.of(INVALID_PARAMETER_VALUE, REQUEST_LIMIT_EXCEEDED);

    public TencentCloudMultimodalResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction);
    }

    @Override
    public RetryException buildFailureStatusCodeException(OutboundRequest request, HttpResult result) {
        int statusCode = result.response().getCode();
        if (statusCode == 500) {
            var errorCode = errorCode(result);
            boolean retry = errorCode == null || NON_RETRYABLE_SERVER_ERROR_CODES.contains(errorCode) == false;
            return new RetryException(retry, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 502) {
            return new RetryException(true, buildError(SERVER_ERROR, request, result));
        }
        return super.buildFailureStatusCodeException(request, result);
    }

    /**
     * Returns the {@code error.code} of an error response body, or {@code null} when the body has none.
     */
    @Nullable
    static String errorCode(HttpResult result) {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (var parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, result.body())) {
            if (parser.map().get("error") instanceof Map<?, ?> error && error.get("code") instanceof String code) {
                return code;
            }
        } catch (Exception e) {
            // not a JSON error body
        }
        return null;
    }
}
