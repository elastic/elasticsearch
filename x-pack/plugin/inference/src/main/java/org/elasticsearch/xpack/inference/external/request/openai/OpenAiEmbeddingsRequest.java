/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiUtils.createOrgHeader;

public class OpenAiEmbeddingsRequest implements Request {

    private final OpenAiAccount account;
    private final OpenAiEmbeddingsRequestEntity entity;
    private final URI uri;

    public OpenAiEmbeddingsRequest(OpenAiAccount account, OpenAiEmbeddingsRequestEntity entity) {
        this.account = Objects.requireNonNull(account);
        this.entity = Objects.requireNonNull(entity);
        this.uri = buildUri(this.account.url());
    }

    private static URI buildUri(URI accountUri) {
        try {
            return accountUri == null ? buildDefaultUri() : accountUri;
        } catch (URISyntaxException e) {
            throw new ElasticsearchStatusException("Failed to construct OpenAI URL", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public HttpRequestBase createRequest() {
        HttpPost httpPost = new HttpPost(uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(Strings.toString(entity).getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(account.apiKey()));

        var org = account.organizationId();
        if (org != null) {
            httpPost.setHeader(createOrgHeader(org));
        }

        return httpPost;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate(double reductionPercentage) {
        var input = Truncator.truncate(entity.input(), reductionPercentage);
        var truncatedEntity = new OpenAiEmbeddingsRequestEntity(input, entity.model(), entity.user());

        return new OpenAiEmbeddingsRequest(account, truncatedEntity);
    }

    // default for testing
    static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(OpenAiUtils.HOST)
            .setPathSegments(OpenAiUtils.VERSION_1, OpenAiUtils.EMBEDDINGS_PATH)
            .build();
    }
}
