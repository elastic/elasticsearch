/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.mistral.response.MistralErrorResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;

/**
 * Handles non-streaming completion responses for Mistral models, extending the OpenAI completion response handler.
 * This class is specifically designed to handle Mistral's error response format.
 */
public class MistralCompletionResponseHandler extends OpenAiChatCompletionResponseHandler {

    /**
     * Constructs a MistralCompletionResponseHandler with the specified request type and response parser.
     *
     * @param requestType The type of request being handled (e.g., "mistral completions").
     * @param parseFunction The function to parse the response.
     */
    public MistralCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, MistralErrorResponseEntity::fromResponse);
    }
}
