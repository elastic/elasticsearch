/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.completion;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

/**
 * Handles streaming chat completion responses and error parsing for Nvidia inference endpoints.
 * This handler is designed to work with the unified Nvidia chat completion API.
 */
public class NvidiaChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String NVIDIA_ERROR = "nvidia_error";
    private static final UnifiedChatCompletionErrorParserContract NVIDIA_ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithStringify(NVIDIA_ERROR);

    /**
     * Constructor for creating a NvidiaChatCompletionResponseHandler with specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public NvidiaChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, NVIDIA_ERROR_PARSER::parse, NVIDIA_ERROR_PARSER);
    }
}
