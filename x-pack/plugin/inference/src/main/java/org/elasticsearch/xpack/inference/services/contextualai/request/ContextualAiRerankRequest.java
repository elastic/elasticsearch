/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class ContextualAiRerankRequest implements Request {

    private static final Logger logger = LogManager.getLogger(ContextualAiRerankRequest.class);

    private final String query;
    private final List<String> documents;
    private final Integer topN;
    private final String instruction;
    private final ContextualAiRerankModel model;

    public ContextualAiRerankRequest(
        String query,
        List<String> documents,
        @Nullable Integer topN,
        @Nullable String instruction,
        ContextualAiRerankModel model
    ) {
        this.query = Objects.requireNonNull(query);
        this.documents = Objects.requireNonNull(documents);
        this.topN = topN;
        this.instruction = instruction;
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        HttpPost httpPost = new HttpPost(model.uri());

        var requestEntity = new ContextualAiRerankRequestEntity(query, documents, getTopN(), instruction, model);
        String requestJson;
        try {
            requestJson = Strings.toString(requestEntity);
            logger.debug("ContextualAI JSON request for inference id [{}]: {}", model.getInferenceEntityId(), requestJson);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize ContextualAI request entity", e);
        }

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestJson.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());

        httpPost.setHeader(createAuthBearerHeader(model.apiKey()));

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
    public Request truncate() {
        // Not applicable for rerank, only used in text embedding requests
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // Not applicable for rerank, only used in text embedding requests
        return null;
    }
}
