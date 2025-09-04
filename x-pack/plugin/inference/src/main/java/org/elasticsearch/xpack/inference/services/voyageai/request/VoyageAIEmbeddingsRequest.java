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
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsServiceSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class VoyageAIEmbeddingsRequest extends VoyageAIRequest {

    private final List<String> input;
    private final InputType inputType;
    private final VoyageAIEmbeddingsModel embeddingsModel;

    public VoyageAIEmbeddingsRequest(List<String> input, InputType inputType, VoyageAIEmbeddingsModel embeddingsModel) {
        this.embeddingsModel = Objects.requireNonNull(embeddingsModel);
        this.input = Objects.requireNonNull(input);
        this.inputType = inputType;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(embeddingsModel.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new VoyageAIEmbeddingsRequestEntity(
                    input,
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

    public VoyageAIEmbeddingsServiceSettings getServiceSettings() {
        return embeddingsModel.getServiceSettings();
    }
}
