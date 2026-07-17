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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRerankRequest;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIRequestUtils.decorateWithAuthHeader;

public class JinaAIRerankRequest implements OutboundRerankRequest {

    private final String query;
    private final List<String> input;
    private final Boolean returnDocuments;
    private final Integer topN;
    private final JinaAIRerankModel model;

    public JinaAIRerankRequest(
        String query,
        List<String> input,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        JinaAIRerankModel model
    ) {
        this.query = Objects.requireNonNull(query);
        this.input = Objects.requireNonNull(input);
        this.returnDocuments = returnDocuments;
        this.topN = topN;
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        HttpPost httpPost = new HttpPost(getURI());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new JinaAIRerankRequestEntity(query, input, returnDocuments, topN, model)).getBytes(StandardCharsets.UTF_8)
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
    public OutboundRequest truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }
}
