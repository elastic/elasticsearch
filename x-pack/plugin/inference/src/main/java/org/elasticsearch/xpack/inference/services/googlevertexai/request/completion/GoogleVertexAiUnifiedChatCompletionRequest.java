/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundUnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class GoogleVertexAiUnifiedChatCompletionRequest implements OutboundUnifiedCompletionRequest {

    private final GoogleVertexAiChatCompletionModel model;
    private final UnifiedChatInput unifiedChatInput;
    private final URI uri;

    public GoogleVertexAiUnifiedChatCompletionRequest(UnifiedChatInput unifiedChatInput, GoogleVertexAiChatCompletionModel model) {
        this.model = Objects.requireNonNull(model);
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
        this.uri = unifiedChatInput.stream() ? model.streamingURI() : model.nonStreamingUri();
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(uri).build();

        ToXContentObject requestEntity;
        requestEntity = model.getServiceSettings()
            .provider()
            .createRequestEntity(unifiedChatInput, extractModelId(), model.getTaskSettings());

        httpPost.setBody(Strings.toString(requestEntity).getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);

        model.authHeaderDecorator().accept(httpPost, model);
        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    /**
     * Extracts the model ID to be used for the request. If the request contains a model ID, it is preferred.
     * Otherwise, the model ID from the configuration is used.
     * @return the model ID to be used for the request
     */
    private String extractModelId() {
        return unifiedChatInput.getRequest().model() != null ? unifiedChatInput.getRequest().model() : model.getServiceSettings().modelId();
    }

    @Override
    public URI getURI() {
        return this.uri;
    }

    @Override
    public OutboundRequest truncate() {
        // No truncation for Google VertexAI Chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for Google VertexAI Chat completions
        return null;
    }

    @Override
    public boolean isStreaming() {
        return unifiedChatInput.stream();
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public TaskType getTaskType() {
        return model.getTaskType();
    }
}
