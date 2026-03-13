/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiEntraIdApiKeySecrets;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretsFactory;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2Secrets;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AzureOpenAiRequestTests extends ESTestCase {

    private static final String TEST_INFERENCE_ID = "test-inference-id";

    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    public void testDecorateWithAuthHeader_apiKeyPresent() {
        var apiKey = randomSecureStringOfLength(10);
        var httpPost = new HttpPost();
        var secretSettings = new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, apiKey, null);
        var secretsApplier = AzureOpenAiSecretsFactory.createSecretsApplier(
            TEST_INFERENCE_ID,
            threadPool,
            secretSettings,
            AzureOpenAiCompletionServiceSettingsTests.createRandom()
        );

        secretsApplier.applyTo(httpPost, ActionListener.noop());
        var apiKeyHeader = httpPost.getFirstHeader(API_KEY_HEADER);

        assertThat(apiKeyHeader.getValue(), equalTo(apiKey.toString()));
    }

    public void testDecorateWithAuthHeader_entraIdPresent() {
        var entraId = randomSecureStringOfLength(10);
        var httpPost = new HttpPost();
        var secretSettings = new AzureOpenAiEntraIdApiKeySecrets(TEST_INFERENCE_ID, null, entraId);
        var secretsApplier = AzureOpenAiSecretsFactory.createSecretsApplier(
            TEST_INFERENCE_ID,
            threadPool,
            secretSettings,
            AzureOpenAiCompletionServiceSettingsTests.createRandom()
        );

        secretsApplier.applyTo(httpPost, ActionListener.noop());
        var authHeader = httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION);

        assertThat(authHeader.getValue(), equalTo("Bearer " + entraId));
    }

    // TODO fix this one, we should have a test where it does set the header
    public void testDecorateWithAuthHeader_oauthClientSecret_doesNotSetAuthHeaders() {
        var secretSettings = new AzureOpenAiOAuth2Secrets(TEST_INFERENCE_ID, randomSecureStringOfLength(10));
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretsFactory.createSecretsApplier(
                TEST_INFERENCE_ID,
                threadPool,
                secretSettings,
                AzureOpenAiCompletionServiceSettingsTests.createRandom()
            )
        );

        assertThat(thrownException.getMessage(), containsString("OAuth2 requires the fields [client_id, tenant_id, scopes], to be set."));
    }
}
