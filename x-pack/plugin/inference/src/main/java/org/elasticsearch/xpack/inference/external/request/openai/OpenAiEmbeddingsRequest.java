/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiUtils.createOrgHeader;

public class OpenAiEmbeddingsRequest implements Request {

    private final Truncator truncator;
    private final OpenAiAccount account;
    private final Truncator.TruncationResult truncationResult;
    private final URI uri;
    private final OpenAiEmbeddingsModel model;

    public OpenAiEmbeddingsRequest(
        Truncator truncator,
        OpenAiAccount account,
        Truncator.TruncationResult input,
        OpenAiEmbeddingsModel model
    ) {
        this.truncator = Objects.requireNonNull(truncator);
        this.account = Objects.requireNonNull(account);
        this.truncationResult = Objects.requireNonNull(input);
        this.uri = buildUri(this.account.url(), "OpenAI", OpenAiEmbeddingsRequest::buildDefaultUri);
        this.model = Objects.requireNonNull(model);
    }

    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new OpenAiEmbeddingsRequestEntity(
                    truncationResult.input(),
                    model.getServiceSettings().modelId(),
                    model.getTaskSettings().user(),
                    model.getServiceSettings().dimensions(),
                    model.getServiceSettings().dimensionsSetByUser()
                )
            ).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(account.apiKey()));

        var org = account.organizationId();
        if (org != null) {
            httpPost.setHeader(createOrgHeader(org));
        }

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());

        return new OpenAiEmbeddingsRequest(truncator, account, truncatedInput, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }

    // default for testing
    static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(OpenAiUtils.HOST)
            .setPathSegments(OpenAiUtils.VERSION_1, OpenAiUtils.EMBEDDINGS_PATH)
            .build();
    }
}
