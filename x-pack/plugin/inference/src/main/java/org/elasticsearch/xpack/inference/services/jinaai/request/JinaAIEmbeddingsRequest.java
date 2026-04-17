/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.request.DenseEmbeddingRequest;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIRequestUtils.decorateWithAuthHeader;

public class JinaAIEmbeddingsRequest implements DenseEmbeddingRequest {

    private final List<InferenceStringGroup> input;
    private final InputType inputType;
    private final JinaAIEmbeddingsModel model;

    public JinaAIEmbeddingsRequest(List<InferenceStringGroup> input, InputType inputType, JinaAIEmbeddingsModel embeddingsModel) {
        this.input = Objects.requireNonNull(input);
        this.inputType = inputType;
        this.model = Objects.requireNonNull(embeddingsModel);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        HttpPost httpPost = new HttpPost(getURI());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new JinaAIEmbeddingsRequestEntity(input, inputType, model)).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        decorateWithAuthHeader(httpPost, model.apiKey());

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
    public Request truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }

    @Override
    public TaskType getTaskType() {
        return model.getTaskType();
    }

    public JinaAIEmbeddingType getEmbeddingType() {
        return model.getServiceSettings().getEmbeddingType();
    }
}
