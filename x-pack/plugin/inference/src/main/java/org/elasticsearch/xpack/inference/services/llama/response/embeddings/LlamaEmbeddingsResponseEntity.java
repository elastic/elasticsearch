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

/**
 * LlamaEmbeddingsResponseEntity is responsible for parsing the response from Llama embeddings requests.
 * It extends HuggingFaceEmbeddingsResponseEntity to handle the expected response format.
 */
public class LlamaEmbeddingsResponseEntity {
    /**
     * Parses the response from a Llama embeddings request and returns the results.
     *
     * @param request the original request that was sent
     * @param response the HTTP result containing the response data
     * @return an InferenceServiceResults object containing the parsed results
     * @throws IOException if there is an error parsing the response
     */
    public static InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        // expected response type is the same as the HuggingFace Embeddings
        return HuggingFaceEmbeddingsResponseEntity.fromResponse(request, response);
    }

}
