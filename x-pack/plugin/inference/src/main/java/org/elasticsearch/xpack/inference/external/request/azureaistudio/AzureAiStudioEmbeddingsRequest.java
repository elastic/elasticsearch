/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaistudio;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModel;

import java.nio.charset.StandardCharsets;

public class AzureAiStudioEmbeddingsRequest extends AzureAiStudioRequest {

    private final AzureAiStudioEmbeddingsModel embeddingsModel;
    private final Truncator.TruncationResult truncationResult;
    private final Truncator truncator;

    public AzureAiStudioEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, AzureAiStudioModel model) {
        super(model);
        this.embeddingsModel = (AzureAiStudioEmbeddingsModel) model;
        this.truncator = truncator;
        this.truncationResult = input;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(this.uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new AzureAiStudioEmbeddingsRequestEntity(embeddingsModel, truncationResult.input()))
                .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        setAuthHeader(httpPost, embeddingsModel);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new AzureAiStudioEmbeddingsRequest(truncator, truncatedInput, embeddingsModel);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }
}
