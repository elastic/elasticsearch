/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class IbmWatsonxChatCompletionRequest implements IbmWatsonxRequest {
    private final IbmWatsonxChatCompletionModel model;
    private final UnifiedChatInput chatInput;

    public IbmWatsonxChatCompletionRequest(UnifiedChatInput chatInput, IbmWatsonxChatCompletionModel model) {
        this.chatInput = Objects.requireNonNull(chatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(model.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new IbmWatsonxChatCompletionRequestEntity(chatInput, model)).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(model.getSecretSettings().apiKey()));

        decorateWithAuth(httpPost);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    public void decorateWithAuth(HttpPost httpPost) {
        IbmWatsonxRequest.decorateWithBearerToken(httpPost, model.getSecretSettings(), model.getInferenceEntityId());
    }

    @Override
    public Request truncate() {
        // No truncation for Ibm WatsonX chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for Ibm WatsonX chat completions
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
}
