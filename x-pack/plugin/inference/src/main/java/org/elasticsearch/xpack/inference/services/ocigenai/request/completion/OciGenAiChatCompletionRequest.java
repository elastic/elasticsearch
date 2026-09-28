/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request.completion;

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
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class OciGenAiChatCompletionRequest implements OutboundUnifiedCompletionRequest {

    private final OciGenAiChatCompletionModel model;
    private final UnifiedChatInput chatInput;

    public OciGenAiChatCompletionRequest(UnifiedChatInput chatInput, OciGenAiChatCompletionModel model) {
        this.chatInput = Objects.requireNonNull(chatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        HttpPost httpPost = new HttpPost(model.uri());

        var entity = new OciGenAiChatCompletionRequestEntity(chatInput, model);
        httpPost.setEntity(new ByteArrayEntity(Strings.toString(entity).getBytes(StandardCharsets.UTF_8)));
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        model.requestSigner().accept(httpPost, model);

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    /**
     * @return the OCI Generative AI model id the request targets, reported in the responses because OCI Generative AI does not echo it
     *         in streaming events
     */
    public String modelId() {
        return model.getServiceSettings().modelId();
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public OutboundRequest truncate() {
        // No truncation for OCI Generative AI chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for OCI Generative AI chat completions
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

    @Override
    public TaskType getTaskType() {
        return model.getTaskType();
    }
}
