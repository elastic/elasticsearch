/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParser;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.response.IbmWatsonxErrorResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

/**
 * Handles streaming chat completion responses and error parsing for Watsonx inference endpoints.
 * Adapts the OpenAI handler to support Watsonx's error schema.
 */
public class IbmWatsonUnifiedChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final UnifiedChatCompletionErrorParser WATSONX_ERROR_PARSER =
        new IbmWatsonxErrorResponseEntity.IbmWatsonxStreamingErrorParser();

    public IbmWatsonUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, IbmWatsonxErrorResponseEntity::fromResponse, WATSONX_ERROR_PARSER);
    }
}
