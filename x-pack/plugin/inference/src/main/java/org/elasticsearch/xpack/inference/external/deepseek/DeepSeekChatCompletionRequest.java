/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.deepseek;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.deepseek.DeepSeekChatCompletionModel;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class DeepSeekChatCompletionRequest implements Request {

    private final DeepSeekChatCompletionModel model;
    private final UnifiedChatInput unifiedChatInput;

    public DeepSeekChatCompletionRequest(UnifiedChatInput unifiedChatInput, DeepSeekChatCompletionModel model) {
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(model.uri());

        httpPost.setEntity(createEntity());

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(model.apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    private ByteArrayEntity createEntity() {
        try (var builder = JsonXContent.contentBuilder()) {
            new DeepSeekChatCompletionRequestEntity(unifiedChatInput, model).toXContent(builder, ToXContent.EMPTY_PARAMS);
            return new ByteArrayEntity(Strings.toString(builder).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to serialize request payload.", e);
        }
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
