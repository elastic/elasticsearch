/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicAccount;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicRequestUtils.createVersionHeader;

public class AnthropicChatCompletionRequest implements Request {

    private final AnthropicAccount account;
    private final List<String> input;
    private final AnthropicChatCompletionModel model;
    private final boolean stream;

    public AnthropicChatCompletionRequest(List<String> input, AnthropicChatCompletionModel model, boolean stream) {
        this.account = AnthropicAccount.of(model);
        this.input = Objects.requireNonNull(input);
        this.model = Objects.requireNonNull(model);
        this.stream = stream;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(account.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new AnthropicChatCompletionRequestEntity(input, model.getServiceSettings(), model.getTaskSettings(), stream))
                .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(AnthropicRequestUtils.createAuthBearerHeader(account.apiKey()));
        httpPost.setHeader(createVersionHeader());

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return account.uri();
    }

    @Override
    public Request truncate() {
        // No truncation for Anthropic completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for Anthropic completions
        return null;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public boolean isStreaming() {
        return stream;
    }

}
