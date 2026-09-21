/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.util.Map;
import java.util.function.Function;

public class OpenAiCompletionResponseHandler extends OpenAiResponseHandler {

    static final String TOKEN_OVERFLOW_ERROR_TYPE = "content_too_large";

    public OpenAiCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, true);
    }

    public OpenAiCompletionResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        Function<HttpResult, ErrorResponse> errorParseFunction
    ) {
        super(requestType, parseFunction, errorParseFunction, true);
    }

    @Override
    protected RetryException buildExceptionHandling429(OutboundRequest outboundRequest, HttpResult result) {
        if (isTokenLimitExceeded(result)) {
            // Token-overflow 429s must not be retried; the request itself must be reduced in size
            return new RetryException(false, buildError(RATE_LIMIT, outboundRequest, result));
        }
        return super.buildExceptionHandling429(outboundRequest, result);
    }

    /**
     * Returns true when the 429 response indicates that the request exceeded the model's context
     * window (a token-overflow condition), rather than a transient rate-limit that can be retried.
     * OpenAI signals this with {@code "error.type": "content_too_large"} in the response body.
     */
    static boolean isTokenLimitExceeded(HttpResult result) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, result.body())
        ) {
            var responseMap = jsonParser.map();
            @SuppressWarnings("unchecked")
            var error = (Map<String, Object>) responseMap.get("error");
            if (error != null) {
                return TOKEN_OVERFLOW_ERROR_TYPE.equals(error.get("type"));
            }
        } catch (Exception e) {
            // swallow — fall through to retry
        }
        return false;
    }
}
