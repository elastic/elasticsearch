/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundDenseEmbeddingRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * HTTP request wrapper for FireworksAI embeddings API calls.
 * Handles request construction, authentication, truncation, and serialization.
 */
public class FireworksAiEmbeddingsRequest implements OutboundDenseEmbeddingRequest {

    private final List<String> input;
    private final FireworksAiEmbeddingsModel model;

    public FireworksAiEmbeddingsRequest(List<String> input, FireworksAiEmbeddingsModel model) {
        this.input = Objects.requireNonNull(input);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(model.uri()).build();

        // Only include dimensions in the request if explicitly set by the user.
        // Some models don't support the dimensions parameter, so we only send it when user configured it.
        Integer dimensions = null;
        if (model.getServiceSettings().dimensionsSetByUser()) {
            dimensions = model.getServiceSettings().dimensions();
        }

        httpPost.setBody(
            Strings.toString(new FireworksAiEmbeddingsRequestEntity(input, model.getServiceSettings().modelId(), dimensions))
                .getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );

        httpPost.setHeader(createAuthBearerHeader(model.apiKey()));

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
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
    public OutboundRequest truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }

    @Override
    public TaskType getTaskType() {
        return model.getTaskType();
    }
}
