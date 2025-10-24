/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.completion;

import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;

/**
 * Handles non-streaming completion responses for Nvidia inference endpoints, extending the OpenAI completion response handler.
 */
public class NvidiaCompletionResponseHandler extends OpenAiChatCompletionResponseHandler {

    /**
     * Constructs a NvidiaCompletionResponseHandler with the specified request type and response parser.
     *
     * @param requestType The type of request being handled.
     * @param parseFunction The function to parse the response.
     */
    public NvidiaCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, ErrorResponse::fromResponse);
    }
}
