/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRerankRequest;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class GoogleVertexAiRerankRequest implements OutboundRerankRequest {

    private final GoogleVertexAiRerankModel model;

    private final String query;

    private final List<String> input;

    private final Boolean returnDocuments;

    private final Integer topN;

    public GoogleVertexAiRerankRequest(
        String query,
        List<String> input,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        GoogleVertexAiRerankModel model
    ) {
        this.model = Objects.requireNonNull(model);
        this.query = Objects.requireNonNull(query);
        this.input = Objects.requireNonNull(input);
        this.returnDocuments = returnDocuments;
        this.topN = topN;
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(model.nonStreamingUri()).build();

        httpPost.setBody(
            Strings.toString(
                new GoogleVertexAiRerankRequestEntity(
                    query,
                    input,
                    returnDocuments,
                    topN != null ? topN : model.getTaskSettings().topN(),
                    model.getServiceSettings().modelId()
                )
            ).getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );

        model.authHeaderDecorator().accept(httpPost, model);

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
    }

    public GoogleVertexAiRerankModel model() {
        return model;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public URI getURI() {
        return model.nonStreamingUri();
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
