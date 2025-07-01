/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.response;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.nio.charset.StandardCharsets;

/**
 * LlamaErrorResponse is responsible for handling error responses from Llama inference services.
 * It extends ErrorResponse to provide specific functionality for Llama errors.
 * An example error response for Not Found error would look like:
 * <pre><code>
 *  {
 *      "detail": "Not Found"
 *  }
 * </code></pre>
 * An example error response for Bad Request error would look like:
 * <pre><code>
 *  {
 *     "error": {
 *         "detail": {
 *             "errors": [
 *                 {
 *                     "loc": [
 *                         "body",
 *                         "model"
 *                     ],
 *                     "msg": "Field required",
 *                     "type": "missing"
 *                 }
 *             ]
 *         }
 *     }
 *  }
 * </code></pre>
 */
public class LlamaErrorResponse extends ErrorResponse {

    public LlamaErrorResponse(String message) {
        super(message);
    }

    public static ErrorResponse fromResponse(HttpResult response) {
        try {
            String errorMessage = new String(response.body(), StandardCharsets.UTF_8);
            return new LlamaErrorResponse(errorMessage);
        } catch (Exception e) {
            // swallow the error
        }
        return ErrorResponse.UNDEFINED_ERROR;
    }
}
