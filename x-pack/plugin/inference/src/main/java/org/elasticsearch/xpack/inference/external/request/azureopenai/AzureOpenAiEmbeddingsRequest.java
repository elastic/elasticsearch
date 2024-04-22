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
import org.elasticsearch.common.ValidationException;
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
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.ENTRA_ID;

public class AzureOpenAiEmbeddingsRequest implements AzureOpenAiRequest {
    private static final String MISSING_AUTHENTICATION_ERROR_MESSAGE =
        "The request does not have any authentication methods set. One of [%s] or [%s] is required.";

    private final Truncator truncator;
    private final AzureOpenAiAccount account;
    private final Truncator.TruncationResult truncationResult;
    private final URI uri;
    private final AzureOpenAiEmbeddingsModel model;

    public AzureOpenAiEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, AzureOpenAiEmbeddingsModel model) {
        this.truncator = Objects.requireNonNull(truncator);
        this.account = AzureOpenAiAccount.fromModel(model);
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
                model.getServiceSettings().dimensionsSetByUser()
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
        } else {
            // should never happen due to the checks on the secret settings, but just in case
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(Strings.format(MISSING_AUTHENTICATION_ERROR_MESSAGE, API_KEY, ENTRA_ID));
            throw validationException;
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

        return new AzureOpenAiEmbeddingsRequest(truncator, truncatedInput, model);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }
}
