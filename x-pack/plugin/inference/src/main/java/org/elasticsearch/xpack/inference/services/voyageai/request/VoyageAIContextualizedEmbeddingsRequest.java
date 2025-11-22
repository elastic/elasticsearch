/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual.VoyageAIContextualEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual.VoyageAIContextualEmbeddingsServiceSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class VoyageAIContextualizedEmbeddingsRequest extends VoyageAIRequest {

    private final List<List<String>> inputs;
    private final InputType inputType;
    private final VoyageAIContextualEmbeddingsModel embeddingsModel;

    public VoyageAIContextualizedEmbeddingsRequest(List<List<String>> inputs, InputType inputType, VoyageAIContextualEmbeddingsModel embeddingsModel) {
        this.embeddingsModel = Objects.requireNonNull(embeddingsModel);
        this.inputs = Objects.requireNonNull(inputs);
        this.inputType = inputType;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(embeddingsModel.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new VoyageAIContextualizedEmbeddingsRequestEntity(
                    inputs,
                    inputType,
                    embeddingsModel.getServiceSettings(),
                    embeddingsModel.getTaskSettings(),
                    embeddingsModel.getServiceSettings().modelId()
                )
            ).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        decorateWithHeaders(httpPost, embeddingsModel);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public String getInferenceEntityId() {
        return embeddingsModel.getInferenceEntityId();
    }

    @Override
    public URI getURI() {
        return embeddingsModel.uri();
    }

    @Override
    public Request truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }

    public VoyageAIContextualEmbeddingsServiceSettings getServiceSettings() {
        return embeddingsModel.getServiceSettings();
    }
}
