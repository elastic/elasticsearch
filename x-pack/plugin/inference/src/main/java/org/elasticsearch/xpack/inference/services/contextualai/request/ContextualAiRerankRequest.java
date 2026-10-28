/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRerankRequest;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class ContextualAiRerankRequest implements OutboundRerankRequest {

    private final String query;
    private final List<String> documents;
    private final Integer topN;
    private final ContextualAiRerankModel model;

    public ContextualAiRerankRequest(String query, List<String> documents, @Nullable Integer topN, ContextualAiRerankModel model) {
        this.query = Objects.requireNonNull(query);
        this.documents = Objects.requireNonNull(documents);
        this.topN = topN;
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(getURI()).build();

        httpPost.setBody(
            Strings.toString(
                new ContextualAiRerankRequestEntity(
                    model.getServiceSettings().modelId(),
                    query,
                    documents,
                    getTopN(),
                    model.getTaskSettings().getInstruction()
                )
            ).getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );

        httpPost.setHeader(createAuthBearerHeader(model.getSecretSettings().apiKey()));

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

    public Integer getTopN() {
        return topN != null ? topN : (model.getTaskSettings() != null ? model.getTaskSettings().getTopN() : null);
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
}
