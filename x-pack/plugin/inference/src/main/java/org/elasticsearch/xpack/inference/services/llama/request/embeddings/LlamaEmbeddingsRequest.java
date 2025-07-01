/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.request.embeddings;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * Llama Embeddings Request
 * This class is responsible for creating a request to the Llama embeddings model.
 * It constructs an HTTP POST request with the necessary headers and body content.
 */
public class LlamaEmbeddingsRequest implements Request {
    private final URI uri;
    private final LlamaEmbeddingsModel model;
    private final String inferenceEntityId;
    private final Truncator.TruncationResult truncationResult;
    private final Truncator truncator;

    /**
     * Constructs a new LlamaEmbeddingsRequest with the specified truncator, input, and model.
     *
     * @param truncator the truncator to handle input truncation
     * @param input the input to be truncated
     * @param model the Llama embeddings model to be used for the request
     */
    public LlamaEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, LlamaEmbeddingsModel model) {
        this.uri = model.uri();
        this.model = model;
        this.inferenceEntityId = model.getInferenceEntityId();
        this.truncator = truncator;
        this.truncationResult = input;
    }

    /**
     * Returns the URI for this request.
     *
     * @return the URI of the Llama embeddings model
     */
    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(this.uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new LlamaEmbeddingsRequestEntity(model.model(), truncationResult.input())).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        if (model.getSecretSettings() instanceof DefaultSecretSettings) {
            var secretSettings = (DefaultSecretSettings) model.getSecretSettings();
            httpPost.setHeader(createAuthBearerHeader(secretSettings.apiKey()));
        }

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new LlamaEmbeddingsRequest(truncator, truncatedInput, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }
}
