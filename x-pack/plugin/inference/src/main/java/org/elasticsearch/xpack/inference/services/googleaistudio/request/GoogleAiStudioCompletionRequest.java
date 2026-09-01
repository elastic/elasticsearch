/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundCompletionRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class GoogleAiStudioCompletionRequest implements OutboundCompletionRequest {
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
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        var httpPost = createHttpPost();
        var requestEntity = Strings.toString(new GoogleAiStudioCompletionRequestEntity(input.getInputs()));

        httpPost.setBody(requestEntity.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    private SimpleHttpRequest createHttpPost() {
        try {
            var uriBuilder = GoogleAiStudioRequestUtils.builderWithApiKeyParameter(uri.getOrCompute(), model.getSecretSettings());
            if (isStreaming()) {
                uriBuilder.addParameter(ALT_PARAM, SSE_VALUE);
            }
            // build via ServiceUtils so the ':' in Google-style method paths (e.g. models/gemini-pro:generateContent) stays unencoded
            return SimpleRequestBuilder.post(ServiceUtils.buildUriPreservingColons(uriBuilder)).build();
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
    public OutboundRequest truncate() {
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
