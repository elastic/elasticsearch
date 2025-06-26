/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.response.embeddings;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.huggingface.response.HuggingFaceEmbeddingsResponseEntity;

import java.io.IOException;

public class LlamaEmbeddingsResponseEntity {
    public static InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        // expected response type is the same as the HuggingFace Embeddings
        return HuggingFaceEmbeddingsResponseEntity.fromResponse(request, response);
    }

}
