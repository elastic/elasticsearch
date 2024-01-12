/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class CohereEmbeddingsRequest implements Request {

    private final Truncator truncator;
    private final CohereAccount account;
    private final Truncator.TruncationResult truncationResult;
    private final URI uri;
    private final CohereEmbeddingsTaskSettings taskSettings;

    public CohereEmbeddingsRequest(
        Truncator truncator,
        CohereAccount account,
        Truncator.TruncationResult input,
        CohereEmbeddingsTaskSettings taskSettings
    ) {
        this.truncator = Objects.requireNonNull(truncator);
        this.account = Objects.requireNonNull(account);
        this.truncationResult = Objects.requireNonNull(input);
        this.uri = buildUri(this.account.url(), "Cohere", CohereEmbeddingsRequest::buildDefaultUri);
        this.taskSettings = Objects.requireNonNull(taskSettings);
    }

    @Override
    public HttpRequestBase createRequest() {
        HttpPost httpPost = new HttpPost(uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new CohereEmbeddingsRequestEntity(truncationResult.input(), taskSettings)).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(account.apiKey()));

        return httpPost;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate() {
        // TODO only do this is the truncate setting is NONE?
        var truncatedInput = truncator.truncate(truncationResult.input());

        return new CohereEmbeddingsRequest(truncator, account, truncatedInput, taskSettings);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }

    // default for testing
    static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(CohereUtils.HOST)
            .setPathSegments(CohereUtils.VERSION_1, CohereUtils.EMBEDDINGS_PATH)
            .build();
    }
}
