/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModel;

import java.nio.charset.StandardCharsets;

public class AzureAiStudioEmbeddingsRequest extends AzureAiStudioRequest {

    private final AzureAiStudioEmbeddingsModel embeddingsModel;
    private final Truncator.TruncationResult truncationResult;
    private final InputType inputType;
    private final Truncator truncator;

    public AzureAiStudioEmbeddingsRequest(
        Truncator truncator,
        Truncator.TruncationResult input,
        InputType inputType,
        AzureAiStudioEmbeddingsModel model
    ) {
        super(model);
        this.embeddingsModel = model;
        this.truncator = truncator;
        this.truncationResult = input;
        this.inputType = inputType;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(this.uri);

        var user = embeddingsModel.getTaskSettings().user();
        var dimensions = embeddingsModel.getServiceSettings().dimensions();
        var dimensionsSetByUser = embeddingsModel.getServiceSettings().dimensionsSetByUser();

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new AzureAiStudioEmbeddingsRequestEntity(truncationResult.input(), inputType, user, dimensions, dimensionsSetByUser)
            ).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        setAuthHeader(httpPost, embeddingsModel);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new AzureAiStudioEmbeddingsRequest(truncator, truncatedInput, inputType, embeddingsModel);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }
}
