/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.http.HttpSettings;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundUnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class IbmWatsonxChatCompletionRequest implements OutboundUnifiedCompletionRequest {
    private final IbmWatsonxChatCompletionModel model;
    private final UnifiedChatInput chatInput;
    private final HttpSettings.ProxySettings proxySettings;

    public IbmWatsonxChatCompletionRequest(
        UnifiedChatInput chatInput,
        IbmWatsonxChatCompletionModel model,
        HttpSettings.ProxySettings proxySettings
    ) {
        this.chatInput = Objects.requireNonNull(chatInput);
        this.model = Objects.requireNonNull(model);
        this.proxySettings = proxySettings;
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(model.uri()).build();

        httpPost.setBody(
            Strings.toString(new IbmWatsonxChatCompletionRequestEntity(chatInput, model)).getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );

        model.authHeaderDecorator().apply(httpPost, model, proxySettings);

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public OutboundRequest truncate() {
        // No truncation for IBM watsonx chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for IBM watsonx chat completions
        return null;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public boolean isStreaming() {
        return chatInput.stream();
    }

    @Override
    public TaskType getTaskType() {
        return model.getTaskType();
    }
}
