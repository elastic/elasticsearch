/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.rerank;

import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.openai.OpenAiResponseHandler;

/**
 * Handles responses for Nvidia rerank requests, parsing the response and handling errors.
 * This class extends {@link OpenAiResponseHandler} to provide specific functionality for Nvidia rerank operation.
 */
public class NvidiaRerankResponseHandler extends OpenAiResponseHandler {

    /**
     * Constructs a new {@link NvidiaRerankResponseHandler} with the specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public NvidiaRerankResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, ErrorResponse::fromResponse, false);
    }
}
