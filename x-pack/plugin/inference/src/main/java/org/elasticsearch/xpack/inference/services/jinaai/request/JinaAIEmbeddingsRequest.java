/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIAccount;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class JinaAIEmbeddingsRequest extends JinaAIRequest {

    private final JinaAIAccount account;
    private final List<String> input;
    private final InputType inputType;
    private final JinaAIEmbeddingsTaskSettings taskSettings;
    private final String model;
    private final String inferenceEntityId;
    private final JinaAIEmbeddingType embeddingType;

    public JinaAIEmbeddingsRequest(List<String> input, InputType inputType, JinaAIEmbeddingsModel embeddingsModel) {
        Objects.requireNonNull(embeddingsModel);

        account = JinaAIAccount.of(embeddingsModel, JinaAIEmbeddingsRequest::buildDefaultUri);
        this.input = Objects.requireNonNull(input);
        this.inputType = inputType;
        taskSettings = embeddingsModel.getTaskSettings();
        model = embeddingsModel.getServiceSettings().getCommonSettings().modelId();
        embeddingType = embeddingsModel.getServiceSettings().getEmbeddingType();
        inferenceEntityId = embeddingsModel.getInferenceEntityId();
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(account.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new JinaAIEmbeddingsRequestEntity(input, inputType, taskSettings, model, embeddingType))
                .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        decorateWithAuthHeader(httpPost, account);

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

    public JinaAIEmbeddingType getEmbeddingType() {
        return embeddingType;
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(JinaAIUtils.HOST)
            .setPathSegments(JinaAIUtils.VERSION_1, JinaAIUtils.EMBEDDINGS_PATH)
            .build();
    }
}
