/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.openai.OpenAiResponseHandler;

/**
 * Handles responses for Mixedbread embeddings requests, parsing the response and handling errors.
 * This class extends {@link OpenAiResponseHandler} to provide specific functionality for Mixedbread embeddings.
 */
public class MixedbreadEmbeddingsResponseHandler extends OpenAiResponseHandler {
    private static final String CONTENT_TOO_LARGE_MESSAGE = "exceeds maximum allowed token size";

    /**
     * Constructs a new {@link MixedbreadEmbeddingsResponseHandler} with the specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public MixedbreadEmbeddingsResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, ErrorResponse::fromResponse, false);
    }

    /**
     * Determines if the given HTTP result indicates that the content is too large.
     *
     * @param result the HTTP result to check
     * @return true if the content is too large, false otherwise
     */
    @Override
    public boolean isContentTooLarge(HttpResult result) {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 413) {
            return true;
        }
        if (statusCode == 400) {
            var errorResponse = ErrorResponse.fromResponse(result);
            return errorResponse != null
                && errorResponse.getErrorMessage() != null
                && errorResponse.getErrorMessage().contains(CONTENT_TOO_LARGE_MESSAGE);
        }
        return false;
    }
}
