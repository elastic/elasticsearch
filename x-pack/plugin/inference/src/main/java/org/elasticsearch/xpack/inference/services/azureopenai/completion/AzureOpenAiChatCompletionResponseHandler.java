/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

/**
 * Handles streaming chat completion responses and error parsing for Azure OpenAI inference endpoints.
 * Adapts the OpenAI handler to support Azure OpenAI's error schema.
 */
public class AzureOpenAiChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String AZURE_OPENAI_ERROR = "azure_openai_error";
    private static final UnifiedChatCompletionErrorParserContract AZURE_OPENAI_ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithStringify(AZURE_OPENAI_ERROR);

    public AzureOpenAiChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, AZURE_OPENAI_ERROR_PARSER::parse, AZURE_OPENAI_ERROR_PARSER);
    }
}
