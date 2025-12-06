/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.util.Map;
import java.util.Objects;

/**
 * A pattern is emerging in how external providers provide error responses.
 *
 * At a minimum, these return:
 * <pre><code>
 * {
 *     "error: {
 *         "message": "(error message)"
 *     }
 * }
 * </code></pre>
 * Others may return additional information such as error codes specific to the service.
 *
 * This currently covers error handling for Azure AI Studio, however this pattern
 * can be used to simplify and refactor handling for Azure OpenAI and OpenAI responses.
 */
public class ErrorMessageResponseEntity extends ErrorResponse {

    public ErrorMessageResponseEntity(String errorMessage) {
        super(errorMessage);
    }

    @SuppressWarnings("unchecked")
    public static ErrorResponse fromResponse(HttpResult response, String defaultMessage) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var responseMap = jsonParser.map();

            var error = (Map<String, Object>) responseMap.get("error");
            if (error != null) {
                var message = (String) error.get("message");
                return new ErrorMessageResponseEntity(Objects.requireNonNullElse(message, defaultMessage));
            }
        } catch (Exception e) {
            // swallow the error
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }

    /**
     * Standard error response parser. This can be overridden for those subclasses that
     * might have a different format
     *
     * @param response the HttpResult
     * @return the error response
     */
    public static ErrorResponse fromResponse(HttpResult response) {
        return fromResponse(response, "");
    }
}
