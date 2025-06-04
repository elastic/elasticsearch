/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class GoogleVertexAiUnifiedChatCompletionRequest implements GoogleVertexAiRequest {

    private final GoogleVertexAiChatCompletionModel model;
    private final UnifiedChatInput unifiedChatInput;

    public GoogleVertexAiUnifiedChatCompletionRequest(UnifiedChatInput unifiedChatInput, GoogleVertexAiChatCompletionModel model) {
        this.model = Objects.requireNonNull(model);
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
    }

    @Override
    public HttpRequest createHttpRequest() {
        var uri = unifiedChatInput.stream() ? model.streamingURI() : model.nonStreamingUri();
        HttpPost httpPost = new HttpPost(uri);

        var requestEntity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        ByteArrayEntity byteEntity = new ByteArrayEntity(Strings.toString(requestEntity).getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        decorateWithAuth(httpPost);
        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    public void decorateWithAuth(HttpPost httpPost) {
        GoogleVertexAiRequest.decorateWithBearerToken(httpPost, model.getSecretSettings());
    }

    @Override
    public URI getURI() {
        return model.nonStreamingUri();
    }

    @Override
    public Request truncate() {
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
}
