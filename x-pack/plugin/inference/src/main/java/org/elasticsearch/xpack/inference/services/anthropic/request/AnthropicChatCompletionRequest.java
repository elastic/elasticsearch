/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundCompletionRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicAccount;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicRequestUtils.createVersionHeader;

public class AnthropicChatCompletionRequest implements OutboundCompletionRequest {

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
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(account.uri()).build();

        httpPost.setBody(
            Strings.toString(new AnthropicChatCompletionRequestEntity(input, model.getServiceSettings(), model.getTaskSettings(), stream))
                .getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );

        httpPost.setHeader(AnthropicRequestUtils.createAuthBearerHeader(account.apiKey()));
        httpPost.setHeader(createVersionHeader());

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    @Override
    public URI getURI() {
        return account.uri();
    }

    @Override
    public OutboundRequest truncate() {
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
