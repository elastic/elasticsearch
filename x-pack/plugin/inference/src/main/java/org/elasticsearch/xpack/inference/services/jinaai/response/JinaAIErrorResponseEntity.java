/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.response;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

public class JinaAIErrorResponseEntity extends ErrorResponse {

    private JinaAIErrorResponseEntity(String errorMessage) {
        super(errorMessage);
    }

    /**
     * Parse an HTTP response into a JinaAIErrorResponseEntity
     *
     * @param response The error response
     * @return An error entity if the response is JSON with a `detail` field containing the error message
     * or null if the response does not contain the message field
     */
    public static ErrorResponse fromResponse(HttpResult response) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var responseMap = jsonParser.map();
            var message = (String) responseMap.get("detail");
            if (message != null) {
                return new JinaAIErrorResponseEntity(message);
            }
        } catch (Exception e) {
            // swallow the error
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }
}
