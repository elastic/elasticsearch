/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.completion;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaService;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaUtils;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * Nvidia Chat Completion Request
 * This class is responsible for creating a request to the Nvidia chat completion model.
 * It constructs an HTTP POST request with the necessary headers and body content.
 */
public class NvidiaChatCompletionRequest implements Request {

    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https")
        .setHost(NvidiaUtils.HOST)
        .setPathSegments(NvidiaUtils.VERSION_1, NvidiaUtils.CHAT_PATH, NvidiaUtils.COMPLETIONS_PATH);
    private final NvidiaChatCompletionModel model;
    private final UnifiedChatInput chatInput;

    public NvidiaChatCompletionRequest(UnifiedChatInput chatInput, NvidiaChatCompletionModel model) {
        this.chatInput = Objects.requireNonNull(chatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(getURI());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new NvidiaChatCompletionRequestEntity(chatInput, model.getServiceSettings().modelId()))
                .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());
        httpPost.setHeader(createAuthBearerHeader(model.getSecretSettings().apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return buildUri(model.getServiceSettings().uri(), NvidiaService.NAME, DEFAULT_URI_BUILDER::build);
    }

    @Override
    public Request truncate() {
        // No truncation for Nvidia chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for Nvidia chat completions
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
