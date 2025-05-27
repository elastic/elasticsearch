/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.mistral.response.MistralErrorResponseEntity;

/**
 * Handles non-streaming chat completion responses for Mistral models, extending the OpenAI chat completion response handler.
 * This class is specifically designed to handle Mistral's error response format.
 */
public class MistralChatCompletionResponseHandler extends OpenAiChatCompletionResponseHandler {

    /**
     * Constructs a MistralChatCompletionResponseHandler with the specified request type and response parser.
     *
     * @param requestType The type of request being handled (e.g., "mistral chat completions").
     * @param parseFunction The function to parse the response.
     */
    public MistralChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, MistralErrorResponseEntity::fromResponse);
    }
}
