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
import org.elasticsearch.xpack.inference.external.http.retry.ErrorMessage;

import java.util.Map;

/**
 * A pattern is emerging in how external providers provide error responses.
 *
 * At a minimum, these return:
 * {
 *     "error: {
 *         "message": "(error message)"
 *     }
 * }
 *
 * Others may return additional information such as error codes specific to the service.
 *
 * This currently covers error handling for Azure AI Studio, however this pattern
 * can be used to simplify and refactor handling for Azure OpenAI and OpenAI responses.
 */
public class ErrorMessageResponseEntity implements ErrorMessage {
    protected String errorMessage;

    public ErrorMessageResponseEntity(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Standard error response parser. This can be overridden for those subclasses that
     * might have a different format
     *
     * @param response the HttpResult
     * @return the error response
     */
    @SuppressWarnings("unchecked")
    public static ErrorMessage fromResponse(HttpResult response) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var responseMap = jsonParser.map();

            var error = (Map<String, Object>) responseMap.get("error");
            if (error != null) {
                var message = (String) error.get("message");
                if (message != null) {
                    return new ErrorMessageResponseEntity(message);
                }
            }
        } catch (Exception e) {
            // swallow the error
        }

        return null;
    }
}
