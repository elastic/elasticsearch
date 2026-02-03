/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.rerank;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
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
public record NvidiaRerankRequest(String query, List<String> input, NvidiaRerankModel model) implements Request {

    public NvidiaRerankRequest {
        Objects.requireNonNull(input);
        Objects.requireNonNull(query);
        Objects.requireNonNull(model);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(getURI());

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new NvidiaRerankRequestEntity(model.getServiceSettings().modelId(), query, input))
                .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());

        httpPost.setHeader(createAuthBearerHeader(model.getSecretSettings().apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
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
