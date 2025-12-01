/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

/**
 * Handles streaming chat completion responses and error parsing for Groq inference endpoints.
 * This handler is designed to work with the Groq chat completion API.
 * Extending the OpenAI unified chat completion response handler to leverage existing functionality.
 */
public class GroqChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String GROQ_ERROR = "groq_error";
    private static final UnifiedChatCompletionErrorParserContract GROQ_ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithStringify(GROQ_ERROR);

    /**
     * Constructor for creating a {@link GroqChatCompletionResponseHandler} with specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public GroqChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, GROQ_ERROR_PARSER::parse, GROQ_ERROR_PARSER);
    }
}
