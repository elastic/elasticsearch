/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiEntraIdApiKeySecrets;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretsSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.oauth.AzureOpenAiOAuth2Secrets;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.oauth.AzureOpenAiOAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AzureOpenAiRequestTests extends ESTestCase {

    public void testDecorateWithAuthHeader_apiKeyPresent() {
        var apiKey = randomSecureStringOfLength(10);
        var httpPost = new HttpPost();
        var secretSettings = new AzureOpenAiEntraIdApiKeySecrets(apiKey, null);

        AzureOpenAiRequest.decorateWithAuthHeader(httpPost, secretSettings);
        var apiKeyHeader = httpPost.getFirstHeader(API_KEY_HEADER);

        assertThat(apiKeyHeader.getValue(), equalTo(apiKey.toString()));
    }

    public void testDecorateWithAuthHeader_entraIdPresent() {
        var entraId = randomSecureStringOfLength(10);
        var httpPost = new HttpPost();
        var secretSettings = new AzureOpenAiEntraIdApiKeySecrets(null, entraId);

        AzureOpenAiRequest.decorateWithAuthHeader(httpPost, secretSettings);
        var authHeader = httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION);

        assertThat(authHeader.getValue(), equalTo("Bearer " + entraId));
    }

    public void testDecorateWithAuthHeader_oauthClientSecret_doesNotSetAuthHeaders() {
        var httpPost = new HttpPost();
        var secretSettings = (AzureOpenAiOAuth2Secrets) AzureOpenAiSecretsSettings.fromMap(
            Map.of(CLIENT_SECRET_FIELD, randomAlphaOfLength(10))
        );

        AzureOpenAiRequest.decorateWithAuthHeader(httpPost, secretSettings);

        assertThat(httpPost.getFirstHeader(API_KEY_HEADER), nullValue());
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION), nullValue());
    }
}
