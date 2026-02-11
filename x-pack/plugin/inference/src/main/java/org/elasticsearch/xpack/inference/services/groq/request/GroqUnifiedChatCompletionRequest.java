/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.groq.GroqUtils;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class GroqUnifiedChatCompletionRequest implements Request {

    private final UnifiedChatInput unifiedChatInput;
    private final GroqChatCompletionModel model;

    public GroqUnifiedChatCompletionRequest(UnifiedChatInput unifiedChatInput, GroqChatCompletionModel model) {
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(model.uri());
        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new GroqUnifiedChatCompletionRequestEntity(unifiedChatInput, model)).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(model.apiKey()));

        var org = model.getServiceSettings().organizationId();
        if (org != null) {
            httpPost.setHeader(GroqUtils.createOrgHeader(org));
        }

        if (model.getTaskSettings().headers() != null) {
            for (var header : model.getTaskSettings().headers().entrySet()) {
                httpPost.setHeader(header.getKey(), header.getValue());
            }
        }

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public Request truncate() {
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
    public boolean isStreaming() {
        return unifiedChatInput.stream();
    }
}
