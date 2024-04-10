/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureopenai;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.azureopenai.AzureOpenAiAccount;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils.API_KEY_HEADER;

public class AzureOpenAiEmbeddingsRequest implements AzureOpenAiRequest {
    private final Truncator truncator;
    private final AzureOpenAiAccount account;
    private final Truncator.TruncationResult truncationResult;
    private final URI uri;
    private final AzureOpenAiEmbeddingsModel model;

    public AzureOpenAiEmbeddingsRequest(
        Truncator truncator,
        AzureOpenAiAccount account,
        Truncator.TruncationResult input,
        AzureOpenAiEmbeddingsModel model
    ) {
        this.truncator = Objects.requireNonNull(truncator);
        this.account = Objects.requireNonNull(account);
        this.truncationResult = Objects.requireNonNull(input);
        this.model = Objects.requireNonNull(model);
        this.uri = model.getUri();
    }

    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(uri);

        String requestEntity = Strings.toString(
            new AzureOpenAiEmbeddingsRequestEntity(
                truncationResult.input(),
                model.getTaskSettings().user(),
                model.getServiceSettings().dimensions(),
                model.getServiceSettings().dimensionsSetByUser(),
                model.getServiceSettings().encodingFormat(),
                model.getServiceSettings().encodingFormatSetByUser()
            )
        );

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType()));

        var entraId = model.getSecretSettings().entraId();
        var apiKey = model.getSecretSettings().apiKey();

        if (entraId != null && entraId.isEmpty() == false) {
            httpPost.setHeader(createAuthBearerHeader(entraId));
        } else if (apiKey != null && apiKey.isEmpty() == false) {
            httpPost.setHeader(new BasicHeader(API_KEY_HEADER, apiKey.toString()));
        }

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return this.uri;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());

        return new AzureOpenAiEmbeddingsRequest(truncator, account, truncatedInput, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }
}
