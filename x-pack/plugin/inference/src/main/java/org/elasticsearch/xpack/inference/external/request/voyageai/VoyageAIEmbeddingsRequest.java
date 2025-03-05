/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.voyageai;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.voyageai.VoyageAIAccount;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class VoyageAIEmbeddingsRequest extends VoyageAIRequest {

    private final VoyageAIAccount account;
    private final List<String> input;
    private final VoyageAIEmbeddingsServiceSettings serviceSettings;
    private final VoyageAIEmbeddingsTaskSettings taskSettings;
    private final String model;
    private final String inferenceEntityId;

    public VoyageAIEmbeddingsRequest(List<String> input, VoyageAIEmbeddingsModel embeddingsModel) {
        Objects.requireNonNull(embeddingsModel);

        account = VoyageAIAccount.of(embeddingsModel);
        this.input = Objects.requireNonNull(input);
        serviceSettings = embeddingsModel.getServiceSettings();
        taskSettings = embeddingsModel.getTaskSettings();
        model = embeddingsModel.getServiceSettings().getCommonSettings().modelId();
        inferenceEntityId = embeddingsModel.getInferenceEntityId();
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(account.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new VoyageAIEmbeddingsRequestEntity(input, serviceSettings, taskSettings, model))
                .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        decorateWithHeaders(httpPost, account);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    @Override
    public URI getURI() {
        return account.uri();
    }

    @Override
    public Request truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }

    public VoyageAIEmbeddingsTaskSettings getTaskSettings() {
        return taskSettings;
    }

    public VoyageAIEmbeddingsServiceSettings getServiceSettings() {
        return serviceSettings;
    }
}
