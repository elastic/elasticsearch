/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.response;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.DETAIL_FIELD;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.MESSAGE_FIELD;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.MSG_FIELD;

public class MistralErrorResponseEntity extends ErrorResponse {

    public MistralErrorResponseEntity(String message) {
        super(message);
    }

    /**
     * Represents a structured error response specifically for non-streaming operations
     * using Mistral API. This is separate from streaming error responses,
     * which are handled by private nested MistralChatCompletionResponseHandler.StreamingMistralErrorResponseEntity.
     * An example error response for Not Found error would look like:
     * <pre><code>
     *     {
     *     "detail": "Not Found"
     *     }
     * </code></pre>
     * An example error response for Bad Request error would look like:
     * <pre><code>
     *     {
     *     "object": "error",
     *     "message": "Invalid model: wrong-model-name",
     *     "type": "invalid_model",
     *     "param": null,
     *     "code": "1500"
     *     }
     * </code></pre>
     * An example error response for Unprocessable Entity error would look like:
     * <pre><code>
     *     {
     *     "object": "error",
     *     "message": {
     *         "detail": [
     *             {
     *                 "type": "greater_than_equal",
     *                 "loc": [
     *                     "body",
     *                     "max_tokens"
     *                 ],
     *                 "msg": "Input should be greater than or equal to 0",
     *                 "input": -10,
     *                 "ctx": {
     *                     "ge": 0
     *                 }
     *             }
     *         ]
     *     },
     *     "type": "invalid_request_error",
     *     "param": null,
     *     "code": null
     *     }
     * </code></pre>
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
            String errorMessage = extractErrorMessage(responseMap);
            if (errorMessage != null) {
                return new MistralErrorResponseEntity(errorMessage);
            }
        } catch (Exception e) {
            // swallow the error
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }

    private static String extractErrorMessage(Map<String, Object> responseMap) {
        Object message = responseMap.get(MESSAGE_FIELD);

        if (message instanceof String stringMessage) {
            return stringMessage;
        }

        if (message instanceof Map<?, ?> messageMap) {
            Object detail = messageMap.get(DETAIL_FIELD);

            if (detail instanceof List<?> detailList && detailList.isEmpty() == false) {
                Object firstError = detailList.get(0);
                if (firstError instanceof Map) {
                    Object msg = ((Map<?, ?>) firstError).get(MSG_FIELD);
                    if (msg instanceof String stringMsg) {
                        return stringMsg;
                    }
                }
            }
        }

        if (responseMap.get(DETAIL_FIELD) instanceof String stringDetail) {
            return stringDetail;
        }

        return "Unknown error response format";
    }

}
