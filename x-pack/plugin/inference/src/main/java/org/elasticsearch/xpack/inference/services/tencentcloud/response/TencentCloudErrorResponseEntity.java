/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.response;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.util.Map;

/**
 * Parses TencentCloud AI Gateway error responses. The response body follows the OpenAI-compatible shape:
 * <pre>
 *   {
 *     "error": { "message": "...", "type": "...", "code": "..." }
 *   }
 * </pre>
 */
public class TencentCloudErrorResponseEntity extends ErrorResponse {

    private TencentCloudErrorResponseEntity(String errorMessage) {
        super(errorMessage);
    }

    @SuppressWarnings("unchecked")
    public static ErrorResponse fromResponse(HttpResult response) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var responseMap = jsonParser.map();
            var error = responseMap.get("error");
            if (error instanceof Map<?, ?> errorMap) {
                var message = (String) ((Map<String, Object>) errorMap).get("message");
                if (message != null) {
                    return new TencentCloudErrorResponseEntity(message);
                }
            }
            var message = (String) responseMap.get("message");
            if (message != null) {
                return new TencentCloudErrorResponseEntity(message);
            }
        } catch (Exception e) {
            // swallow the error, return UNDEFINED_ERROR below
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }
}
