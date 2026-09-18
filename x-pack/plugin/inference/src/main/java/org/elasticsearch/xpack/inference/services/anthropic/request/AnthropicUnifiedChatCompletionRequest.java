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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundUnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicAccount;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicRequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicRequestUtils.createVersionHeader;

/**
 * Outbound HTTP request for the Anthropic Messages API used by the {@code chat_completion} task type.
 * Serializes the unified chat input to Anthropic's body shape via {@link AnthropicUnifiedChatCompletionRequestEntity}
 * and attaches the {@code x-api-key} and {@code anthropic-version} headers expected by the API.
 */
public class AnthropicUnifiedChatCompletionRequest implements OutboundUnifiedCompletionRequest {

    private final AnthropicAccount account;
    private final UnifiedChatInput unifiedChatInput;
    private final AnthropicChatCompletionModel model;

    public AnthropicUnifiedChatCompletionRequest(UnifiedChatInput unifiedChatInput, AnthropicChatCompletionModel model) {
        this.account = AnthropicAccount.of(model);
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(account.uri()).build();

        var entity = new AnthropicUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            model.getServiceSettings().modelId(),
            model.getTaskSettings()
        );
        httpPost.setBody(Strings.toString(entity).getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);

        httpPost.setHeader(createAuthBearerHeader(account.apiKey()));
        httpPost.setHeader(createVersionHeader());

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    @Override
    public URI getURI() {
        return account.uri();
    }

    @Override
    public OutboundRequest truncate() {
        // No truncation for Anthropic chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for Anthropic chat completions
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
        return unifiedChatInput.stream();
    }
}
