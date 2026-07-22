/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundUnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * AI21 Chat Completion Request
 * This class is responsible for creating a request to the AI21 chat completion model.
 * It constructs an HTTP POST request with the necessary headers and body content.
 */
public class Ai21ChatCompletionRequest implements OutboundUnifiedCompletionRequest {

    private final Ai21ChatCompletionModel model;
    private final UnifiedChatInput chatInput;

    public Ai21ChatCompletionRequest(UnifiedChatInput chatInput, Ai21ChatCompletionModel model) {
        this.chatInput = Objects.requireNonNull(chatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        HttpPost httpPost = new HttpPost(model.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new Ai21ChatCompletionRequestEntity(chatInput, model.getServiceSettings().modelId()))
                .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());
        httpPost.setHeader(createAuthBearerHeader(model.getSecretSettings().apiKey()));

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public OutboundRequest truncate() {
        // No truncation for AI21 chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for AI21 chat completions
        return null;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public TaskType getTaskType() {
        return model.getTaskType();
    }

    @Override
    public boolean isStreaming() {
        return chatInput.stream();
    }
}
