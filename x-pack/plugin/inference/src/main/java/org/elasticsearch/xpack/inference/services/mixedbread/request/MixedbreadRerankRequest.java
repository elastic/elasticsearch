/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankModel;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class MixedbreadRerankRequest extends MixedbreadRequest {
    private final String query;
    private final List<String> input;
    private final Boolean returnDocuments;
    private final Integer topN;
    private final MixedbreadRerankModel rerankModel;

    public MixedbreadRerankRequest(
        MixedbreadRerankModel model,
        String query,
        List<String> input,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN
    ) {
        super(model);
        this.rerankModel = Objects.requireNonNull(model);
        this.query = query;
        this.input = Objects.requireNonNull(input);
        this.returnDocuments = returnDocuments;
        this.topN = topN;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(this.uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(Strings.toString(createRequestEntity()).getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        setAuthHeader(httpPost, rerankModel);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public Request truncate() {
        // Not applicable for rerank, only used in text embedding requests
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // Not applicable for rerank, only used in text embedding requests
        return null;
    }

    private MixedbreadRerankRequestEntity createRequestEntity() {
        var taskSettings = rerankModel.getTaskSettings();
        return new MixedbreadRerankRequestEntity(rerankModel.model(), query, input, topN, returnDocuments, taskSettings);
    }

    public Integer getTopN() {
        return topN != null ? topN : rerankModel.getTaskSettings().topK();
    }
}
