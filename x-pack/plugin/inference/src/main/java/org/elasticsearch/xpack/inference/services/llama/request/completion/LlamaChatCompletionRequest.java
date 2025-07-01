/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.request.completion;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * Llama Chat Completion Request
 * This class is responsible for creating a request to the Llama chat completion model.
 * It constructs an HTTP POST request with the necessary headers and body content.
 */
public class LlamaChatCompletionRequest implements Request {

    private final LlamaChatCompletionModel model;
    private final UnifiedChatInput chatInput;

    /**
     * Constructs a new LlamaChatCompletionRequest with the specified chat input and model.
     *
     * @param chatInput the chat input containing the messages and parameters for the completion request
     * @param model the Llama chat completion model to be used for the request
     */
    public LlamaChatCompletionRequest(UnifiedChatInput chatInput, LlamaChatCompletionModel model) {
        this.chatInput = Objects.requireNonNull(chatInput);
        this.model = Objects.requireNonNull(model);
    }

    /**
     * Returns the chat input for this request.
     *
     * @return the chat input containing the messages and parameters
     */
    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(model.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new LlamaChatCompletionRequestEntity(chatInput, model)).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());
        if (model.getSecretSettings() instanceof DefaultSecretSettings) {
            var secretSettings = (DefaultSecretSettings) model.getSecretSettings();
            httpPost.setHeader(createAuthBearerHeader(secretSettings.apiKey()));
        }

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public Request truncate() {
        // No truncation for Llama chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for Llama chat completions
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
}
