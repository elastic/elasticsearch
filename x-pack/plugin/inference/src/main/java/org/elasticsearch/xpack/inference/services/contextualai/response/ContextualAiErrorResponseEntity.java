/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.response;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

/**
 * Error response entity for ContextualAI API calls.
 */
public class ContextualAiErrorResponseEntity extends ErrorResponse {

    private ContextualAiErrorResponseEntity(String errorMessage) {
        super(errorMessage);
    }

    public static ErrorResponse fromResponse(HttpResult response) {
        // Simple error handling - just return the status line as the error message
        return new ContextualAiErrorResponseEntity(response.response().getStatusLine().toString());
    }
}
