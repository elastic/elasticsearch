/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.response;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.BaseResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;

import java.io.IOException;

/**
 * Parses the FireworksAI embeddings API response.
 * FireworksAI uses an OpenAI-compatible embeddings API format, so we reuse the OpenAI response parser.
 */
public class FireworksAiEmbeddingsResponseEntity extends BaseResponseEntity {
    @Override
    protected InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        // FireworksAI uses the same response format as OpenAI embeddings
        return OpenAiEmbeddingsResponseEntity.fromResponse(request, response);
    }
}
