/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request.rerank;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankModel;
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankTaskSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class HuggingFaceRerankRequest implements Request {

    private final HuggingFaceAccount account;
    private final String query;
    private final List<String> input;
    private final Boolean returnDocuments;
    private final Integer topN;
    private final TaskSettings taskSettings;
    private final HuggingFaceRerankModel model;
    private final String inferenceEntityId;

    public HuggingFaceRerankRequest(
        String query,
        List<String> input,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        HuggingFaceRerankModel model
    ) {
        Objects.requireNonNull(model);

        this.account = HuggingFaceAccount.of(model);
        this.input = Objects.requireNonNull(input);
        this.query = Objects.requireNonNull(query);
        this.returnDocuments = returnDocuments;
        this.topN = topN;
        taskSettings = model.getTaskSettings();
        this.model = model;
        inferenceEntityId = model.getInferenceEntityId();
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(account.uri());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new HuggingFaceRerankRequestEntity(
                    query,
                    input,
                    returnDocuments,
                    topN != null ? topN : model.getTaskSettings().getTopNDocumentsOnly(),
                    (HuggingFaceRerankTaskSettings) taskSettings,
                    model.getServiceSettings().modelId()
                )
            ).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        decorateWithAuth(httpPost);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    public void decorateWithAuth(HttpPost httpPost) {
        httpPost.setHeader(createAuthBearerHeader(model.apiKey()));
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    @Override
    public URI getURI() {
        return account.uri();
    }

    public Integer getTopN() {
        return topN;
    }

    @Override
    public Request truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }
}
