/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.googleaistudio;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class GoogleAiStudioCompletionRequest implements GoogleAiStudioRequest {

    private final List<String> input;

    private final URI uri;

    private final GoogleAiStudioCompletionModel model;

    public GoogleAiStudioCompletionRequest(List<String> input, GoogleAiStudioCompletionModel model) {
        this.input = input;
        this.model = Objects.requireNonNull(model);
        this.uri = model.uri();
    }

    @Override
    public HttpRequest createHttpRequest() {
        var httpPost = new HttpPost(uri);
        var requestEntity = Strings.toString(new GoogleAiStudioCompletionRequestEntity(input));

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        GoogleAiStudioRequest.decorateWithApiKeyParameter(httpPost, model.getSecretSettings());

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return this.uri;
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
}
