/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.request.completion;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.groq.GroqUtils;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * Groq Chat Completion Request
 * This class is responsible for creating a request to the Groq chat completion model.
 * It constructs an HTTP POST request with the necessary headers and body content.
 */
public class GroqChatCompletionRequest implements Request {

    private static final URI GROQ_CHAT_COMPLETION_URI;

    static {
        try {
            GROQ_CHAT_COMPLETION_URI = new URIBuilder().setScheme("https")
                .setHost(GroqUtils.HOST)
                .setPathSegments(GroqUtils.OPENAI_PATH, GroqUtils.VERSION_1_PATH, GroqUtils.CHAT_PATH, GroqUtils.COMPLETIONS_PATH)
                .build();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(Strings.format("Unable to build Groq URL. Reason: %s", e.getReason()), e);
        }
    }

    private final GroqChatCompletionModel model;
    private final UnifiedChatInput chatInput;

    public GroqChatCompletionRequest(UnifiedChatInput chatInput, GroqChatCompletionModel model) {
        this.chatInput = Objects.requireNonNull(chatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(getURI());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new GroqChatCompletionRequestEntity(chatInput, model.getServiceSettings().modelId()))
                .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());
        httpPost.setHeader(createAuthBearerHeader(model.getSecretSettings().apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return GROQ_CHAT_COMPLETION_URI;
    }

    @Override
    public Request truncate() {
        // No truncation for Groq chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for Groq chat completions
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
