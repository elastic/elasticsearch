/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class AzureOpenAiChatCompletionRequest implements AzureOpenAiRequest {

    private final UnifiedChatInput chatInput;

    private final AzureOpenAiCompletionModel model;

    public AzureOpenAiChatCompletionRequest(UnifiedChatInput chatInput, AzureOpenAiCompletionModel model) {
        this.chatInput = chatInput;
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public HttpRequest createHttpRequest() {
        var httpPost = new HttpPost(getURI());
        var requestEntity = Strings.toString(new AzureOpenAiChatCompletionRequestEntity(chatInput, model.getTaskSettings().user()));

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        AzureOpenAiRequest.decorateWithAuthHeader(httpPost, model.getSecretSettings());

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return model.getUri();
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
    public Request truncate() {
        // No truncation for Azure OpenAI completion
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for Azure OpenAI completion
        return null;
    }
}
