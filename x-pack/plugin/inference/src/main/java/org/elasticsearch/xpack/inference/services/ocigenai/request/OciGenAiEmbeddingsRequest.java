/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundDenseEmbeddingRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class OciGenAiEmbeddingsRequest implements OutboundDenseEmbeddingRequest {

    private final Truncator truncator;
    private final Truncator.TruncationResult truncationResult;
    private final OciGenAiEmbeddingsModel model;
    private final InputType inputType;

    public OciGenAiEmbeddingsRequest(
        Truncator truncator,
        Truncator.TruncationResult input,
        @Nullable InputType inputType,
        OciGenAiEmbeddingsModel model
    ) {
        this.truncator = Objects.requireNonNull(truncator);
        this.truncationResult = Objects.requireNonNull(input);
        this.model = Objects.requireNonNull(model);
        this.inputType = inputType;
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        HttpPost httpPost = new HttpPost(model.uri());

        var entity = new OciGenAiEmbeddingsRequestEntity(
            truncationResult.input(),
            model.getServiceSettings(),
            OciGenAiEmbeddingsRequestEntity.resolveInputType(inputType, model.getTaskSettings()),
            model.getTaskSettings().getTruncation()
        );
        httpPost.setEntity(new ByteArrayEntity(Strings.toString(entity).getBytes(StandardCharsets.UTF_8)));
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        model.requestSigner().accept(httpPost, model);

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
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
    public OutboundRequest truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new OciGenAiEmbeddingsRequest(truncator, truncatedInput, inputType, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }

    @Override
    public TaskType getTaskType() {
        return model.getTaskType();
    }
}
