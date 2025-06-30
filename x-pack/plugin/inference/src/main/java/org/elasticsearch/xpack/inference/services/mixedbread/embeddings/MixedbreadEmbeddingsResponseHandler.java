/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.mixedbread.response.MixedbreadErrorResponse;
import org.elasticsearch.xpack.inference.services.openai.OpenAiResponseHandler;

public class MixedbreadEmbeddingsResponseHandler extends OpenAiResponseHandler {
    /**
     * Constructs a new MixedbreadEmbeddingsResponseHandler with the specified request type and response parser.
     *
     * @param requestType the type of request this handler will process
     * @param parseFunction the function to parse the response
     */
    public MixedbreadEmbeddingsResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, MixedbreadErrorResponse::fromResponse, false);
    }
}
