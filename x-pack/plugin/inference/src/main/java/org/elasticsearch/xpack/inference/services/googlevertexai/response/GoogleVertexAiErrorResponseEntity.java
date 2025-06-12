/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.response;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.util.Map;
import java.util.Objects;

public class GoogleVertexAiErrorResponseEntity extends ErrorResponse {

    private GoogleVertexAiErrorResponseEntity(String errorMessage) {
        super(errorMessage);
    }

    /**
     * An example error response for invalid auth would look like
     * <code>
     *     {
     *     "error": {
     *         "code": 401,
     *         "message": "some error message",
     *         "status": "UNAUTHENTICATED",
     *         "details": [
     *             {
     *                 "@type": "type.googleapis.com/google.rpc.ErrorInfo",
     *                 "reason": "CREDENTIALS_MISSING",
     *                 "domain": "googleapis.com",
     *                 "metadata": {
     *                     "method": "google.cloud.aiplatform.v1.PredictionService.Predict",
     *                     "service": "aiplatform.googleapis.com"
     *                 }
     *             }
     *         ]
     *     }
     * }
     * </code>
     *
     * @param response The error response
     * @return An error entity if the response is JSON with the above structure
     * or {@link ErrorResponse#UNDEFINED_ERROR} if the error field wasn't found
     */
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
                return new GoogleVertexAiErrorResponseEntity(Objects.requireNonNullElse(message, ""));
            }
        } catch (Exception e) {
            // swallow the error
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }
}
