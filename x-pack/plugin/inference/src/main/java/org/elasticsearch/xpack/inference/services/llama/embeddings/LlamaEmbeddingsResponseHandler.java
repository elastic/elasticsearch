/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.llama.response.LlamaErrorResponse;
import org.elasticsearch.xpack.inference.services.openai.OpenAiResponseHandler;

/**
 * Handles responses for Llama embeddings requests, parsing the response and handling errors.
 * This class extends OpenAiResponseHandler to provide specific functionality for Llama embeddings.
 */
public class LlamaEmbeddingsResponseHandler extends OpenAiResponseHandler {

    /**
     * Constructs a new LlamaEmbeddingsResponseHandler with the specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public LlamaEmbeddingsResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, LlamaErrorResponse::fromResponse, false);
    }
}
