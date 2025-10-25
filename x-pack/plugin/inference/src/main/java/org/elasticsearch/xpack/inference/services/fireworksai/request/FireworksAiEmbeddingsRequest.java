/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * HTTP request wrapper for FireworksAI embeddings API calls.
 * Handles request construction, authentication, truncation, and serialization.
 */
public class FireworksAiEmbeddingsRequest implements Request {

    private final Truncator truncator;
    private final Truncator.TruncationResult truncationResult;
    private final FireworksAiEmbeddingsModel model;

    public FireworksAiEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, FireworksAiEmbeddingsModel model) {
        this.truncator = Objects.requireNonNull(truncator);
        this.truncationResult = Objects.requireNonNull(input);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(model.uri());

        Integer dimensions = model.getTaskSettings() != null ? model.getTaskSettings().dimensions() : null;
        if (dimensions == null) {
            dimensions = model.getServiceSettings().dimensions();
        }

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new FireworksAiEmbeddingsRequestEntity(truncationResult.input(), model.getServiceSettings().modelId(), dimensions)
            ).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(model.apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());

        return new FireworksAiEmbeddingsRequest(truncator, truncatedInput, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }
}
