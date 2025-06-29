/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.llama.response.LlamaErrorResponse;
import org.elasticsearch.xpack.inference.services.openai.OpenAiResponseHandler;

public class LlamaEmbeddingsResponseHandler extends OpenAiResponseHandler {

    public LlamaEmbeddingsResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, LlamaErrorResponse::fromResponse, false);
    }
}
