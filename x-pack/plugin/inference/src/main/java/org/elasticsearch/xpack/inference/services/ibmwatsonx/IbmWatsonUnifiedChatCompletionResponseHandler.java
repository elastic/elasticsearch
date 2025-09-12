/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.response.IbmWatsonxErrorResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import static org.elasticsearch.xpack.inference.services.ibmwatsonx.response.IbmWatsonxErrorResponseEntity.WATSONX_ERROR_PARSER;

/**
 * Handles streaming chat completion responses and error parsing for Watsonx inference endpoints.
 * Adapts the OpenAI handler to support Watsonx's error schema.
 */
public class IbmWatsonUnifiedChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    public IbmWatsonUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, IbmWatsonxErrorResponseEntity::fromResponse, WATSONX_ERROR_PARSER);
    }
}
