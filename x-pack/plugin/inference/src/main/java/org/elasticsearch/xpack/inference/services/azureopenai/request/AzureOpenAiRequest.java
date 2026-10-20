/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiModel;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public abstract class AzureOpenAiRequest<M extends AzureOpenAiModel> implements OutboundRequest {

    protected final M model;
    private final AzureOpenAiTaskSettings<?> taskSettings;
    private final String requestEntity;

    protected AzureOpenAiRequest(M model, AzureOpenAiTaskSettings<?> taskSettings, String requestEntity) {
        this.model = Objects.requireNonNull(model);
        this.taskSettings = Objects.requireNonNull(taskSettings);
        this.requestEntity = Objects.requireNonNull(requestEntity);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        var httpPost = SimpleRequestBuilder.post(getURI()).build();

        httpPost.setBody(requestEntity.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);

        var headers = taskSettings.headers();
        if (headers.mapValue().isPresent()) {
            for (var entry : headers.mapValue().get().entrySet()) {
                httpPost.setHeader(entry.getKey(), entry.getValue());
            }
        }

        model.secretsApplier().applyTo(httpPost, listener.delegateFailureAndWrap((httpRequestActionListener, simpleHttpRequest) -> {
            httpRequestActionListener.onResponse(new HttpRequest(simpleHttpRequest, getInferenceEntityId()));
        }));
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public URI getURI() {
        return model.getUri();
    }

    @Override
    public OutboundRequest truncate() {
        // Default implementation: no truncation. Subclasses may override to apply truncation if needed.
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // Default implementation: no truncation was applied, so no truncation info is available.
        return null;
    }
}
