/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.mistral.response.MistralErrorResponseHelper;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

/**
 * Handles streaming chat completion responses and error parsing for Mistral inference endpoints.
 * Adapts the OpenAI handler to support Mistral's error schema.
 */
public class MistralUnifiedChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    /**
     * Constructs a MistralUnifiedChatCompletionResponseHandler with the specified request type and response parser.
     *
     * @param requestType The type of request being handled (e.g., "mistral completions").
     * @param parseFunction The function to parse the response.
     */
    public MistralUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, MistralErrorResponseHelper::fromResponse, MistralErrorResponseHelper.ERROR_PARSER);
    }
}
