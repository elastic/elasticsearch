/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.googlevertexai;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class GoogleVertexAiRerankRequest implements GoogleVertexAiRequest {

    private final GoogleVertexAiRerankModel model;

    private final String query;

    private final List<String> input;

    public GoogleVertexAiRerankRequest(String query, List<String> input, GoogleVertexAiRerankModel model) {
        this.model = Objects.requireNonNull(model);
        this.query = Objects.requireNonNull(query);
        this.input = Objects.requireNonNull(input);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(model.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new GoogleVertexAiRerankRequestEntity(query, input, model.getServiceSettings().modelId(), model.getTaskSettings().topN())
            ).getBytes(StandardCharsets.UTF_8)
        );

        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        decorateWithAuth(httpPost);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    public void decorateWithAuth(HttpPost httpPost) {
        GoogleVertexAiRequest.decorateWithBearerToken(httpPost, model.getSecretSettings());
    }

    public GoogleVertexAiRerankModel model() {
        return model;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
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
}
