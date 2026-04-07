/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.response;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.BaseResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;

import java.io.IOException;

public class MistralEmbeddingsResponseEntity extends BaseResponseEntity {
    @Override
    protected InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        // expected response type is the same as the Open AI Embeddings
        return OpenAiEmbeddingsResponseEntity.fromResponse(request, response);
    }
}
