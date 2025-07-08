/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;

/**
 * Functional interface for building provider-specific chat completion errors.
 * This interface is used to create exceptions that are specific to the chat completion service being used.
 */
@FunctionalInterface
public interface ChatCompletionErrorBuilder {

    /**
     * Builds a provider-specific chat completion error based on the given parameters.
     *
     * @param errorResponse The error response received from the service.
     * @param errorMessage A custom error message to include in the exception.
     * @param restStatus The HTTP status code associated with the error.
     * @return An instance of {@link UnifiedChatCompletionException} representing the error.
     */
    UnifiedChatCompletionException buildProviderSpecificChatCompletionError(
        ErrorResponse errorResponse,
        String errorMessage,
        RestStatus restStatus
    );
}
