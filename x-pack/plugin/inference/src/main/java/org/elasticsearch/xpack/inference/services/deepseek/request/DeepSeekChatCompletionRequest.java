/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.deepseek.DeepSeekChatCompletionModel;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class DeepSeekChatCompletionRequest implements Request {
    private static final Logger logger = LogManager.getLogger(DeepSeekChatCompletionRequest.class);
    private static final String MODEL_FIELD = "model";
    private static final String MAX_TOKENS = "max_tokens";

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
        model.apiKey()
            .ifPresentOrElse(
                apiKey -> httpPost.setHeader(createAuthBearerHeader(apiKey)),
                () -> logger.debug("No auth token present in request, sending without auth...")
            );

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    private ByteArrayEntity createEntity() {
        var modelId = Objects.requireNonNullElseGet(unifiedChatInput.getRequest().model(), model::model);
        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            new UnifiedChatCompletionRequestEntity(unifiedChatInput).toXContent(
                builder,
                UnifiedCompletionRequest.withMaxTokens(modelId, ToXContent.EMPTY_PARAMS)
            );
            builder.endObject();
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
