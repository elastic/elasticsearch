/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.huggingface;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorMessage;

public record HuggingFaceErrorResponseEntity(String message) implements ErrorMessage {
    /**
     * An example error response for invalid auth would look like
     * <code>
     *     {
     *       "error": "A valid user token is required"
     *     }
     * </code>
     *
     *
     * @param response The error response
     * @return An error entity if the response is JSON with the above structure
     * or null if the response does not contain the error field
     */
    public static HuggingFaceErrorResponseEntity fromResponse(HttpResult response) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var responseMap = jsonParser.map();
            var error = (String) responseMap.get("error");
            if (error != null) {
                return new HuggingFaceErrorResponseEntity(error);
            }
        } catch (Exception e) {
            // swallow the error
        }

        return null;
    }

    @Override
    public String getErrorMessage() {
        return message;
    }
}
