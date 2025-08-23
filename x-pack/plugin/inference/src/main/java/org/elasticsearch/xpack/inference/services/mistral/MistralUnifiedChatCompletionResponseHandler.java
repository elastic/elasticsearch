/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

/**
 * Handles streaming chat completion responses and error parsing for Mistral inference endpoints.
 * Adapts the OpenAI handler to support Mistral's error schema.
 */
public class MistralUnifiedChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String MISTRAL_ERROR = "mistral_error";

    private static final UnifiedChatCompletionErrorParserContract ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithStringify(MISTRAL_ERROR);

    /**
     * Constructs a MistralUnifiedChatCompletionResponseHandler with the specified request type and response parser.
     *
     * @param requestType The type of request being handled (e.g., "mistral completions").
     * @param parseFunction The function to parse the response.
     */
    public MistralUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, ERROR_PARSER::parse, ERROR_PARSER);
    }
}
