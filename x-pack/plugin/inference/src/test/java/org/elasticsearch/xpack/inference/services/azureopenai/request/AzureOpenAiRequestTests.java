/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings;

import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.ENTRA_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiRequest.MISSING_AUTHENTICATION_ERROR_MESSAGE;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AzureOpenAiRequestTests extends ESTestCase {

    public void testDecorateWithAuthHeader_apiKeyPresent() {
        var apiKey = randomSecureStringOfLength(10);
        var httpPost = new HttpPost();
        var secretSettings = new AzureOpenAiSecretSettings(apiKey, null);

        AzureOpenAiRequest.decorateWithAuthHeader(httpPost, secretSettings);
        var apiKeyHeader = httpPost.getFirstHeader(API_KEY_HEADER);

        assertThat(apiKeyHeader.getValue(), equalTo(apiKey.toString()));
    }

    public void testDecorateWithAuthHeader_entraIdPresent() {
        var entraId = randomSecureStringOfLength(10);
        var httpPost = new HttpPost();
        var secretSettings = new AzureOpenAiSecretSettings(null, entraId);

        AzureOpenAiRequest.decorateWithAuthHeader(httpPost, secretSettings);
        var authHeader = httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION);

        assertThat(authHeader.getValue(), equalTo("Bearer " + entraId));
    }

    public void testDecorateWithAuthHeader_entraIdAndApiKeyMissing_throwMissingAuthValidationException() {
        var httpPost = new HttpPost();
        var secretSettingsMock = mock(AzureOpenAiSecretSettings.class);

        when(secretSettingsMock.entraId()).thenReturn(null);
        when(secretSettingsMock.apiKey()).thenReturn(null);

        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiRequest.decorateWithAuthHeader(httpPost, secretSettingsMock)
        );
        assertTrue(exception.getMessage().contains(Strings.format(MISSING_AUTHENTICATION_ERROR_MESSAGE, API_KEY, ENTRA_ID)));
    }
}
