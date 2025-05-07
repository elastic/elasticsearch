/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request.completion;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.services.huggingface.completion.HuggingFaceChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * This class is responsible for creating Hugging Face chat completions HTTP requests.
 * It handles the preparation of the HTTP request with the necessary headers and body.
 */
public class HuggingFaceUnifiedChatCompletionRequest implements Request {

    private final HuggingFaceAccount account;
    private final HuggingFaceChatCompletionModel model;
    private final UnifiedChatInput unifiedChatInput;

    public HuggingFaceUnifiedChatCompletionRequest(UnifiedChatInput unifiedChatInput, HuggingFaceChatCompletionModel model) {
        this.account = HuggingFaceAccount.of(model);
        this.model = Objects.requireNonNull(model);
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
    }

    /**
     * Creates an HTTP request to the Hugging Face API for chat completions.
     * The request includes the necessary headers and the input data as a JSON entity.
     *
     * @return an HttpRequest object containing the HTTP POST request
     */
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(getURI());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new HuggingFaceUnifiedChatCompletionRequestEntity(unifiedChatInput, model)).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());
        httpPost.setHeader(createAuthBearerHeader(model.apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    public URI getURI() {
        return account.uri();
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public Request truncate() {
        // Truncation is not applicable for chat completion requests
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // Truncation is not applicable for chat completion requests
        return null;
    }

    @Override
    public boolean isStreaming() {
        return unifiedChatInput.stream();
    }
}
