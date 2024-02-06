/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.openai;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorMessage;

import java.util.Map;

public class OpenAiErrorResponseEntity implements ErrorMessage {

    private final String errorMessage;

    private OpenAiErrorResponseEntity(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * An example error response for invalid auth would look like
     * <code>
     *     {
     *       "error": {
     *         "message": "You didn't provide an API key...",
     *         "type": "invalid_request_error",
     *         "param": null,
     *         "code": null
     *       }
     *     }
     * </code>
     *
     *
     * @param response The error response
     * @return An error entity if the response is JSON with the above structure
     * or null if the response does not contain the error.message field
     */
    @SuppressWarnings("unchecked")
    public static OpenAiErrorResponseEntity fromResponse(HttpResult response) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var responseMap = jsonParser.map();
            var error = (Map<String, Object>) responseMap.get("error");
            if (error != null) {
                var message = (String) error.get("message");
                if (message != null) {
                    return new OpenAiErrorResponseEntity(message);
                }
            }
        } catch (Exception e) {
            // swallow the error
        }

        return null;
    }
}
