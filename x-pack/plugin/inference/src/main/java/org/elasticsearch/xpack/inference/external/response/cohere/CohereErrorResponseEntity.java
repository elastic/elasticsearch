/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.cohere;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorMessage;

public class CohereErrorResponseEntity implements ErrorMessage {

    private final String errorMessage;

    private CohereErrorResponseEntity(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * An example error response for invalid auth would look like
     * <code>
     *     {
     *       "message": "invalid request: total number of texts must be at most 96 - received 97"
     *     }
     * </code>
     *
     *
     * @param response The error response
     * @return An error entity if the response is JSON with the above structure
     * or null if the response does not contain the message field
     */
    public static CohereErrorResponseEntity fromResponse(HttpResult response) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var responseMap = jsonParser.map();
            var message = (String) responseMap.get("message");
            if (message != null) {
                return new CohereErrorResponseEntity(message);
            }
        } catch (Exception e) {
            // swallow the error
        }

        return null;
    }
}
