/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.completion;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

/**
 * Handles streaming chat completion responses and error parsing for Llama inference endpoints.
 * This handler is designed to work with the unified Llama chat completion API.
 */
public class LlamaChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String LLAMA_ERROR = "llama_error";
    /**
     * Example error response for Bad Request error would look like:
     * <pre><code>
     *  {
     *      "error": {
     *          "message": "400: Invalid value: Model 'llama3.12:3b' not found"
     *      }
     *  }
     * </code></pre>
     *
     * This parser will simply convert the entire object into a string.
     */
    private static final UnifiedChatCompletionErrorParserContract LLAMA_STREAM_ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithStringify(LLAMA_ERROR);

    /**
     * Constructor for creating a LlamaChatCompletionResponseHandler with specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public LlamaChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, LLAMA_STREAM_ERROR_PARSER::parse, LLAMA_STREAM_ERROR_PARSER);
    }
}
