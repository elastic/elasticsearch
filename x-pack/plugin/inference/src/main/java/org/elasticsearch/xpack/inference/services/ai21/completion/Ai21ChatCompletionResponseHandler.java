/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.completion;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

/**
 * Handles streaming chat completion responses and error parsing for AI21 inference endpoints.
 * Adapts the OpenAI handler to support AI21's error schema.
 */
public class Ai21ChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String AI_21_ERROR = "ai21_error";
    private static final UnifiedChatCompletionErrorParserContract AI_21_ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithStringify(AI_21_ERROR);

    public Ai21ChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, AI_21_ERROR_PARSER::parse, AI_21_ERROR_PARSER);
    }
}
