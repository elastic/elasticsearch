/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundDenseEmbeddingRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.decorateWithAuthHeader;

/**
 * Request to TencentCloud {@code POST /v1/multimodal-embeddings} for one batch of inputs.
 */
public class TencentCloudMultimodalEmbeddingsRequest implements OutboundDenseEmbeddingRequest {

    private final List<InferenceString> inputs;
    @Nullable
    private final String promptName;
    private final TencentCloudEmbeddingsModel model;

    public TencentCloudMultimodalEmbeddingsRequest(
        List<InferenceString> inputs,
        @Nullable String promptName,
        TencentCloudEmbeddingsModel model
    ) {
        this.inputs = Objects.requireNonNull(inputs);
        this.promptName = promptName;
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(getURI()).build();

        var entity = new TencentCloudMultimodalEmbeddingsRequestEntity(inputs, model.getServiceSettings().modelId(), promptName);
        httpPost.setBody(Strings.toString(entity).getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);

        decorateWithAuthHeader(httpPost, model.getSecretSettings().apiKey());

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    /**
     * Returns the inputs of this batch in request order.
     */
    public List<InferenceString> getInputs() {
        return inputs;
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
    public TaskType getTaskType() {
        return model.getTaskType();
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }
}
