/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.request.embeddings;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundDenseEmbeddingRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class MistralEmbeddingsRequest implements OutboundDenseEmbeddingRequest {
    private final URI uri;
    private final MistralEmbeddingsModel embeddingsModel;
    private final String inferenceEntityId;
    private final Truncator.TruncationResult truncationResult;
    private final Truncator truncator;

    public MistralEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, MistralEmbeddingsModel model) {
        this.uri = model.uri();
        this.embeddingsModel = model;
        this.inferenceEntityId = model.getInferenceEntityId();
        this.truncator = truncator;
        this.truncationResult = input;
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(this.uri).build();

        httpPost.setBody(
            Strings.toString(new MistralEmbeddingsRequestEntity(embeddingsModel.getServiceSettings().modelId(), truncationResult.input()))
                .getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );

        httpPost.setHeader(createAuthBearerHeader(embeddingsModel.getSecretSettings().apiKey()));

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public OutboundRequest truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new MistralEmbeddingsRequest(truncator, truncatedInput, embeddingsModel);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    @Override
    public TaskType getTaskType() {
        return embeddingsModel.getTaskType();
    }
}
