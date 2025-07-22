/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.response;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponse;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class IbmWatsonxErrorResponseEntity extends UnifiedChatCompletionErrorResponse {
    private static final String WATSONX_ERROR = "watsonx_error";

    private IbmWatsonxErrorResponseEntity(String errorMessage) {
        super(errorMessage, WATSONX_ERROR, null, null);
    }

    public static class IbmWatsonxStreamingErrorParser implements UnifiedChatCompletionErrorParser {

        @Override
        public UnifiedChatCompletionErrorResponse parse(HttpResult result) {
            return fromResponse(result);
        }

        @Override
        public UnifiedChatCompletionErrorResponse parse(String response) {
            return executeGenericParser(IbmWatsonxErrorResponseEntity::doParse, createStringXContentParserFunction(response));
        }
    }

    public static UnifiedChatCompletionErrorResponse fromResponse(HttpResult result) {
        return executeGenericParser(IbmWatsonxErrorResponseEntity::doParse, createHttpResultXContentParserFunction(result));
    }

    private static Optional<UnifiedChatCompletionErrorResponse> doParse(XContentParser parser) throws IOException {
        var responseMap = parser.map();
        @SuppressWarnings("unchecked")
        var error = (Map<String, Object>) responseMap.get("error");
        if (error != null) {
            var message = (String) error.get("message");
            return Optional.of(new IbmWatsonxErrorResponseEntity(Objects.requireNonNullElse(message, "")));
        }

        return Optional.of(UnifiedChatCompletionErrorResponse.UNDEFINED_ERROR);
    }
}
