/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

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
import org.elasticsearch.xpack.inference.external.request.RequestUtils;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Outbound request for TencentCloud AI Gateway {@code POST /v1/chat/completions}.
 * The Gateway is fully OpenAI-compatible; the request body is produced by {@link TencentCloudChatCompletionRequestEntity}.
 */
public class TencentCloudChatCompletionRequest implements OutboundUnifiedCompletionRequest {

    private final TencentCloudChatCompletionModel model;
    private final UnifiedChatInput unifiedChatInput;

    public TencentCloudChatCompletionRequest(UnifiedChatInput unifiedChatInput, TencentCloudChatCompletionModel model) {
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(model.uri()).build();

        httpPost.setBody(
            Strings.toString(new TencentCloudChatCompletionRequestEntity(unifiedChatInput, model)).getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );

        RequestUtils.decorateWithAuthHeader(httpPost, model.getSecretSettings().apiKey());

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public OutboundRequest truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
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
