/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.response;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

/**
 * Error response entity for FireworksAI API calls.
 * Handles error parsing from HTTP responses with non-2xx status codes.
 */
public class FireworksAiErrorResponseEntity extends ErrorResponse {

    private FireworksAiErrorResponseEntity(String errorMessage) {
        super(errorMessage);
    }

    /**
     * Creates an ErrorResponse from an HTTP result.
     *
     * @param response the HTTP response containing error information
     * @return an ErrorResponse with the error message
     */
    public static ErrorResponse fromResponse(HttpResult response) {
        // Simple error handling - return the status line as the error message
        // FireworksAI may return error details in the response body, but for now
        // we use the status line for consistency with other services
        return new FireworksAiErrorResponseEntity(response.response().getStatusLine().toString());
    }
}
