/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureopenai;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class AzureOpenAiCompletionRequest implements AzureOpenAiRequest {

    private final List<String> input;

    private final URI uri;

    private final AzureOpenAiCompletionModel model;

    private final boolean stream;

    public AzureOpenAiCompletionRequest(List<String> input, AzureOpenAiCompletionModel model, boolean stream) {
        this.input = input;
        this.model = Objects.requireNonNull(model);
        this.uri = model.getUri();
        this.stream = stream;
    }

    @Override
    public HttpRequest createHttpRequest() {
        var httpPost = new HttpPost(uri);
        var requestEntity = Strings.toString(new AzureOpenAiCompletionRequestEntity(input, model.getTaskSettings().user(), isStreaming()));

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        AzureOpenAiRequest.decorateWithAuthHeader(httpPost, model.getSecretSettings());

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return this.uri;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public boolean isStreaming() {
        return stream;
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
