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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
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
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(model.uri());

        var requestEntity = new ContextualAiRerankRequestEntity(query, documents, getTopN(), instruction, model);
        String requestJson;
        try {
            requestJson = Strings.toString(requestEntity);
            logger.debug("ContextualAI JSON Request: {}", requestJson);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize ContextualAI request entity", e);
        }

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestJson.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());

        decorateWithAuth(httpPost);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    void decorateWithAuth(HttpPost httpPost) {
        SecureString apiKey = model.apiKey();
        if (apiKey != null) {
            httpPost.setHeader(createAuthBearerHeader(apiKey));
        }
    }

    @Override
    public String getInferenceEntityId() {
        return model != null ? model.getInferenceEntityId() : "unknown";
    }

    @Override
    public URI getURI() {
        return model != null ? model.uri() : null;
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
