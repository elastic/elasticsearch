/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.response;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

public class HuggingFaceErrorResponseEntity extends ErrorResponse {

    public HuggingFaceErrorResponseEntity(String message) {
        super(message);
    }

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
     * or {@link ErrorResponse#UNDEFINED_ERROR} if the error field wasn't found
     */
    public static ErrorResponse fromResponse(HttpResult response) {
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

        return ErrorResponse.UNDEFINED_ERROR;
    }
}
