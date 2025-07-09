/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.response;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.nio.charset.StandardCharsets;

/**
 * Represents an error response entity for Mistral inference services.
 * This class extends ErrorResponse and provides a method to create an instance
 * from an HttpResult, attempting to read the body as a UTF-8 string.
 * An example error response for Not Found error would look like:
 * <pre><code>
 * {
 *     "detail": "Not Found"
 * }
 * </code></pre>
 * An example error response for Bad Request error would look like:
 * <pre><code>
 * {
 *     "object": "error",
 *     "message": "Invalid model: wrong-model-name",
 *     "type": "invalid_model",
 *     "param": null,
 *     "code": "1500"
 * }
 * </code></pre>
 * An example error response for Unauthorized error would look like:
 * <pre><code>
 * {
 *     "message": "Unauthorized",
 *     "request_id": "ad95a2165083f20b490f8f78a14bb104"
 * }
 * </code></pre>
 * An example error response for Unprocessable Entity error would look like:
 * <pre><code>
 * {
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
 * }
 * </code></pre>
 */
public class MistralErrorResponse extends ErrorResponse {

    public MistralErrorResponse(String message) {
        super(message);
    }

    /**
     * Creates an ErrorResponse from the given HttpResult.
     * Attempts to read the body as a UTF-8 string and constructs a MistralErrorResponseEntity.
     * If reading fails, returns a generic UNDEFINED_ERROR.
     *
     * @param response the HttpResult containing the error response
     * @return an ErrorResponse instance
     */
    public static ErrorResponse fromResponse(HttpResult response) {
        try {
            String errorMessage = new String(response.body(), StandardCharsets.UTF_8);
            return new MistralErrorResponse(errorMessage);
        } catch (Exception e) {
            // swallow the error
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }
}
