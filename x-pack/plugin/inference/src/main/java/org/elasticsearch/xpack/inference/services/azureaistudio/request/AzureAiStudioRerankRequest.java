/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRerankRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.rerank.AzureAiStudioRerankModel;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class AzureAiStudioRerankRequest extends AzureAiStudioRequest implements OutboundRerankRequest {
    private final String query;
    private final List<String> input;
    private final Boolean returnDocuments;
    private final Integer topN;
    private final AzureAiStudioRerankModel rerankModel;

    public AzureAiStudioRerankRequest(
        AzureAiStudioRerankModel model,
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
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(this.uri).build();

        httpPost.setBody(Strings.toString(createRequestEntity()).getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);

        setAuthHeader(httpPost, rerankModel);

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    @Override
    public OutboundRequest truncate() {
        // Not applicable for rerank, only used in text embedding requests
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // Not applicable for rerank, only used in text embedding requests
        return null;
    }

    private AzureAiStudioRerankRequestEntity createRequestEntity() {
        return new AzureAiStudioRerankRequestEntity(query, input, returnDocuments, topN, rerankModel.getTaskSettings());
    }
}
