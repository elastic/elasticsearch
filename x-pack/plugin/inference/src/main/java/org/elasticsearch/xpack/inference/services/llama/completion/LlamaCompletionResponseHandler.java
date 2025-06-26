/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.completion;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.llama.response.LlamaErrorResponse;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;

/**
 * Handles non-streaming completion responses for Llama models, extending the OpenAI completion response handler.
 * This class is specifically designed to handle Llama's error response format.
 */
public class LlamaCompletionResponseHandler extends OpenAiChatCompletionResponseHandler {

    /**
     * Constructs a LlamaCompletionResponseHandler with the specified request type and response parser.
     *
     * @param requestType The type of request being handled (e.g., "llama completions").
     * @param parseFunction The function to parse the response.
     */
    public LlamaCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, LlamaErrorResponse::fromResponse);
    }
}
