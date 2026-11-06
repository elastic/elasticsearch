/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.rerank;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRerankRequest;
import org.elasticsearch.xpack.inference.services.nvidia.rerank.NvidiaRerankModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * Represents a request to the Nvidia rerank service.
 * This class constructs the HTTP request with the necessary headers and body content.
 *
 * @param query the query string to rerank against
 * @param input the list of input documents to be reranked
 * @param model the Nvidia rerank model configuration
 */
public record NvidiaRerankRequest(String query, List<String> input, NvidiaRerankModel model) implements OutboundRerankRequest {

    public NvidiaRerankRequest {
        Objects.requireNonNull(input);
        Objects.requireNonNull(query);
        Objects.requireNonNull(model);
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(getURI()).build();

        httpPost.setBody(
            Strings.toString(new NvidiaRerankRequestEntity(model.getServiceSettings().modelId(), query, input))
                .getBytes(StandardCharsets.UTF_8),
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
        return model.getServiceSettings().uri();
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
