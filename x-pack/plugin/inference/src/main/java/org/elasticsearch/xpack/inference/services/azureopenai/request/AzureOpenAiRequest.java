/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiModel;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.ENTRA_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;

public abstract class AzureOpenAiRequest implements Request {

    public static final String MISSING_AUTHENTICATION_ERROR_MESSAGE =
        "The request does not have any authentication methods set. One of [%s] or [%s] is required.";

    private final AzureOpenAiModel model;
    private final AzureOpenAiTaskSettings<?> taskSettings;
    private final String requestEntity;

    protected AzureOpenAiRequest(AzureOpenAiModel model, AzureOpenAiTaskSettings<?> taskSettings, String requestEntity) {
        this.model = Objects.requireNonNull(model);
        this.taskSettings = Objects.requireNonNull(taskSettings);
        this.requestEntity = Objects.requireNonNull(requestEntity);
    }

    @Override
    public HttpRequest createHttpRequest() {
        var httpPost = new HttpPost(getURI());

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        decorateWithAuthHeader(httpPost, model.getSecretSettings());

        var headers = taskSettings.headers();
        if (headers != null && headers.headersMap() != null) {
            for (var entry : headers.headersMap().entrySet()) {
                httpPost.setHeader(entry.getKey(), entry.getValue());
            }
        }

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    // Default for testing
    static void decorateWithAuthHeader(HttpPost httpPost, AzureOpenAiSecretSettings secretSettings) {
        httpPost.setHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType()));

        var entraId = secretSettings.entraId();
        var apiKey = secretSettings.apiKey();

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
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public URI getURI() {
        return model.getUri();
    }

    @Override
    public Request truncate() {
        // No truncation for Azure OpenAI completion
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for Azure OpenAI completion
        return null;
    }
}
