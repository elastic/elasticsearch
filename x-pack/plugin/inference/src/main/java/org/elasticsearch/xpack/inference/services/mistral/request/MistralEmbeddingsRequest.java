/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class MistralEmbeddingsRequest implements Request {
    private final URI uri;
    private final MistralEmbeddingsModel embeddingsModel;
    private final String inferenceEntityId;
    private final Truncator.TruncationResult truncationResult;
    private final Truncator truncator;

    public MistralEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, MistralEmbeddingsModel model) {
        this.uri = model.uri();
        this.embeddingsModel = model;
        this.inferenceEntityId = model.getInferenceEntityId();
        this.truncator = truncator;
        this.truncationResult = input;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(this.uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new MistralEmbeddingsRequestEntity(embeddingsModel.model(), truncationResult.input()))
                .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(embeddingsModel.getSecretSettings().apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new MistralEmbeddingsRequest(truncator, truncatedInput, embeddingsModel);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }
}
