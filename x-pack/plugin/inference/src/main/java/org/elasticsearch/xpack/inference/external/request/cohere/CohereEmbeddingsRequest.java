/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class CohereEmbeddingsRequest implements Request {

    private final CohereAccount account;
    private final List<String> input;
    private final URI uri;
    private final CohereEmbeddingsTaskSettings taskSettings;
    private final String model;
    private final CohereEmbeddingType embeddingType;
    private final String inferenceEntityId;

    public CohereEmbeddingsRequest(CohereAccount account, List<String> input, CohereEmbeddingsModel embeddingsModel) {
        Objects.requireNonNull(embeddingsModel);

        this.account = Objects.requireNonNull(account);
        this.input = Objects.requireNonNull(input);
        uri = buildUri(this.account.url(), "Cohere", CohereEmbeddingsRequest::buildDefaultUri);
        taskSettings = embeddingsModel.getTaskSettings();
        model = embeddingsModel.getServiceSettings().getCommonSettings().modelId();
        embeddingType = embeddingsModel.getServiceSettings().getEmbeddingType();
        inferenceEntityId = embeddingsModel.getInferenceEntityId();
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new CohereEmbeddingsRequestEntity(input, taskSettings, model, embeddingType)).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(account.apiKey()));
        httpPost.setHeader(CohereUtils.createRequestSourceHeader());

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }

    // default for testing
    static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(CohereUtils.HOST)
            .setPathSegments(CohereUtils.VERSION_1, CohereUtils.EMBEDDINGS_PATH)
            .build();
    }
}
