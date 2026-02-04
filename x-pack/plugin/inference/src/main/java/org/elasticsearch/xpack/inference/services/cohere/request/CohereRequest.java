/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.services.cohere.CohereService;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public abstract class CohereRequest implements Request, ToXContentObject {

    public static void decorateWithAuthHeader(HttpPost request, CohereAccount account) {
        request.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        request.setHeader(createAuthBearerHeader(account.apiKey()));
        request.setHeader(CohereUtils.createRequestSourceHeader());
    }

    protected final CohereAccount account;
    private final String inferenceEntityId;
    private final String modelId;
    private final boolean stream;

    protected CohereRequest(CohereAccount account, String inferenceEntityId, @Nullable String modelId, boolean stream) {
        this.account = account;
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
        this.modelId = modelId; // model is optional in the v1 api
        this.stream = stream;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(getURI());

        ByteArrayEntity byteEntity = new ByteArrayEntity(Strings.toString(this).getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        decorateWithAuthHeader(httpPost, account);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    @Override
    public boolean isStreaming() {
        return stream;
    }

    @Override
    public URI getURI() {
        return buildUri(account.baseUri());
    }

    /**
     * Returns the URL path segments.
     * @return List of segments that make up the path of the request.
     */
    protected abstract List<String> pathSegments();

    private URI buildUri(URI baseUri) {
        try {
            return new URIBuilder(baseUri).setPathSegments(pathSegments()).build();
        } catch (URISyntaxException e) {
            throw new ElasticsearchStatusException(
                Strings.format("Failed to construct %s URL", CohereService.NAME),
                RestStatus.BAD_REQUEST,
                e
            );
        }
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    public Request truncate() {
        // no truncation
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // no truncation
        return null;
    }
}
