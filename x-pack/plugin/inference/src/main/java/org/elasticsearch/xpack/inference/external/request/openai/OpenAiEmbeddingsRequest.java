/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class OpenAiEmbeddingsRequest implements Request {
    // TODO this should create the actual request to send via the http client
    // similar to IncidentEvent

    // TODO this should be given an openai account so it can access the api key

    private final OpenAiAccount account;
    private final OpenAiEmbeddingsRequestEntity entity;

    public OpenAiEmbeddingsRequest(OpenAiAccount account, OpenAiEmbeddingsRequestEntity entity) {
        this.account = Objects.requireNonNull(account);
        this.entity = Objects.requireNonNull(entity);
    }

    // TODO pass in the request timeout somewhere
    public HttpUriRequest createRequest() {
        try {
            URI uri = new URIBuilder().setScheme("https")
                .setHost(OpenAiConstants.HOST)
                .setPathSegments(OpenAiConstants.VERSION_1, OpenAiConstants.EMBEDDINGS_PATH)
                .build();

            HttpPost httpPost = new HttpPost(uri);

            ByteArrayEntity byteEntity = new ByteArrayEntity(Strings.toString(entity).getBytes(StandardCharsets.UTF_8));
            httpPost.setEntity(byteEntity);

            httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
            httpPost.setHeader(apiKeyHeader());

            return httpPost;
        } catch (URISyntaxException e) {
            throw new ElasticsearchStatusException("Failed to construct openai URL", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private Header apiKeyHeader() {
        return new BasicHeader("Authorization", "Bearer " + account.getApiKey().toString());
    }
}
