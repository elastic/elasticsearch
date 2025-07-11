/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.response;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionExceptionConvertible;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class IbmWatsonxErrorResponseEntity extends ErrorResponse implements UnifiedChatCompletionExceptionConvertible {

    private static final String WATSONX_ERROR = "watsonx_error";

    private IbmWatsonxErrorResponseEntity(String errorMessage) {
        super(errorMessage);
    }

    @SuppressWarnings("unchecked")
    public static ErrorResponse fromResponse(HttpResult response) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var responseMap = jsonParser.map();
            var error = (Map<String, Object>) responseMap.get("error");
            if (error != null) {
                var message = (String) error.get("message");
                return new IbmWatsonxErrorResponseEntity(Objects.requireNonNullElse(message, ""));
            }
        } catch (Exception e) {
            // swallow the error
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }

    @Override
    public UnifiedChatCompletionException toUnifiedChatCompletionException(String errorMessage, RestStatus restStatus) {
        return new UnifiedChatCompletionException(restStatus, errorMessage, WATSONX_ERROR, restStatus.name().toLowerCase(Locale.ROOT));
    }
}
