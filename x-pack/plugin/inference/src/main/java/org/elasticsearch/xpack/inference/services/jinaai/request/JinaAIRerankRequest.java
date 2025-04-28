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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIAccount;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModel;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class JinaAIRerankRequest extends JinaAIRequest {

    private final JinaAIAccount account;
    private final String query;
    private final List<String> input;
    private final Boolean returnDocuments;
    private final Integer topN;
    private final JinaAIRerankTaskSettings taskSettings;
    private final String model;
    private final String inferenceEntityId;

    public JinaAIRerankRequest(
        String query,
        List<String> input,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        JinaAIRerankModel model
    ) {
        Objects.requireNonNull(model);

        this.account = JinaAIAccount.of(model, JinaAIRerankRequest::buildDefaultUri);
        this.input = Objects.requireNonNull(input);
        this.query = Objects.requireNonNull(query);
        this.returnDocuments = returnDocuments;
        this.topN = topN;
        taskSettings = model.getTaskSettings();
        this.model = model.getServiceSettings().modelId();
        inferenceEntityId = model.getInferenceEntityId();
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(account.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new JinaAIRerankRequestEntity(query, input, returnDocuments, topN, taskSettings, model))
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

    public static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(JinaAIUtils.HOST)
            .setPathSegments(JinaAIUtils.VERSION_1, JinaAIUtils.RERANK_PATH)
            .build();
    }
}
