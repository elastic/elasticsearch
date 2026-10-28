/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.request;

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
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.openai.OpenAiUtils.createOrgHeader;

public class OpenAiUnifiedChatCompletionRequest implements OutboundUnifiedCompletionRequest {

    private final OpenAiChatCompletionModel model;
    private final UnifiedChatInput unifiedChatInput;

    public OpenAiUnifiedChatCompletionRequest(UnifiedChatInput unifiedChatInput, OpenAiChatCompletionModel model) {
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(model.uri()).build();

        httpPost.setBody(
            Strings.toString(new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model)).getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );

        var org = model.rateLimitServiceSettings().organizationId();
        if (org != null) {
            httpPost.setHeader(createOrgHeader(org));
        }

        if (model.getTaskSettings().headers() != null) {
            for (var header : model.getTaskSettings().headers().entrySet()) {
                httpPost.setHeader(header.getKey(), header.getValue());
            }
        }

        model.secretsApplier()
            .applyTo(httpPost, listener.delegateFailureAndWrap((l, req) -> l.onResponse(new HttpRequest(req, getInferenceEntityId()))));
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public OutboundRequest truncate() {
        // No truncation for OpenAI chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for OpenAI chat completions
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

    @Override
    public void onAuthenticationFailure() {
        model.secretsApplier().onAuthenticationFailure();
    }
}
