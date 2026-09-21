/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRerankRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class OciGenAiRerankRequest implements OutboundRerankRequest {

    private final String query;
    private final List<String> documents;
    private final Integer topN;
    private final Boolean returnDocuments;
    private final OciGenAiRerankModel model;

    /**
     * @param topN            the request level top N, overriding the task settings when non-null
     * @param returnDocuments the request level return documents flag, overriding the task settings when non-null
     */
    public OciGenAiRerankRequest(
        String query,
        List<String> documents,
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments,
        OciGenAiRerankModel model
    ) {
        this.query = Objects.requireNonNull(query);
        this.documents = Objects.requireNonNull(documents);
        this.model = Objects.requireNonNull(model);
        this.topN = topN != null ? topN : model.getTaskSettings().getTopN();
        this.returnDocuments = returnDocuments != null ? returnDocuments : model.getTaskSettings().getReturnDocuments();
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        HttpPost httpPost = new HttpPost(model.uri());

        var entity = new OciGenAiRerankRequestEntity(query, documents, model.getServiceSettings(), topN, returnDocuments);
        httpPost.setEntity(new ByteArrayEntity(Strings.toString(entity).getBytes(StandardCharsets.UTF_8)));
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        model.requestSigner().accept(httpPost, model);

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
