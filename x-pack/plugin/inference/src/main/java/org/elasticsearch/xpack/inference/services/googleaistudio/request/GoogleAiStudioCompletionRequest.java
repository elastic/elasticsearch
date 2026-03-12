/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class GoogleAiStudioCompletionRequest implements GoogleAiStudioRequest {
    private static final String ALT_PARAM = "alt";
    private static final String SSE_VALUE = "sse";

    private final ChatCompletionInput input;

    private final LazyInitializable<URI, RuntimeException> uri;

    private final GoogleAiStudioCompletionModel model;

    public GoogleAiStudioCompletionRequest(ChatCompletionInput input, GoogleAiStudioCompletionModel model) {
        this.input = Objects.requireNonNull(input);
        this.model = Objects.requireNonNull(model);
        this.uri = new LazyInitializable<>(() -> model.uri(input.stream()));
    }

    @Override
    public HttpRequest createHttpRequest() {
        var httpPost = createHttpPost();
        var requestEntity = Strings.toString(new GoogleAiStudioCompletionRequestEntity(input.getInputs()));

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    private HttpPost createHttpPost() {
        try {
            var uriBuilder = GoogleAiStudioRequest.builderWithApiKeyParameter(uri.getOrCompute(), model.getSecretSettings());
            if (isStreaming()) {
                uriBuilder.addParameter(ALT_PARAM, SSE_VALUE);
            }
            return new HttpPost(uriBuilder.build());
        } catch (Exception e) {
            ValidationException validationException = new ValidationException(e);
            validationException.addValidationError(e.getMessage());
            throw validationException;
        }
    }

    @Override
    public URI getURI() {
        return uri.getOrCompute();
    }

    @Override
    public Request truncate() {
        // No truncation for Google AI Studio completion
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for Google AI Studio completion
        return null;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public boolean isStreaming() {
        return input.stream();
    }
}
