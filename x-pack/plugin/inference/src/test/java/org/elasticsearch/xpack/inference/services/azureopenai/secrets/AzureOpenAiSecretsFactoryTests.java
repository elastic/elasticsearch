/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.secrets.NoopSecretsApplier;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2SettingsTests;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionServiceSettingsTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiOAuth2Secrets.USE_CLIENT_SECRET_ERROR;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureOpenAiSecretsFactoryTests extends ESTestCase {
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

    public void testCreateSecretsApplier_NullSecrets_ReturnsNoopSecretsApplier() {
        var secretsApplier = AzureOpenAiSecretsFactory.createSecretsApplier(
            TEST_INFERENCE_ID,
            threadPool,
            null,
            AzureOpenAiCompletionServiceSettingsTests.createRandomWithoutOAuth2()
        );

        assertThat(secretsApplier, sameInstance(NoopSecretsApplier.INSTANCE));

        var listener = new TestPlainActionFuture<HttpRequestBase>();
        var httpPost = new HttpPost();
        secretsApplier.applyTo(httpPost, listener);

        var resultRequest = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(resultRequest, sameInstance(httpPost));
        assertThat(resultRequest.getAllHeaders(), emptyArray());
    }

    public void testCreateSecretsApplier_ApiKey() {
        var apiKey = randomSecureStringOfLength(10);
        var httpPost = new HttpPost();
        var secretSettings = new AzureOpenAiEntraIdApiKeySecrets(apiKey, null);
        var secretsApplier = AzureOpenAiSecretsFactory.createSecretsApplier(
            TEST_INFERENCE_ID,
            threadPool,
            secretSettings,
            AzureOpenAiCompletionServiceSettingsTests.createRandomWithoutOAuth2()
        );

        assertNotNull(secretsApplier);

        secretsApplier.applyTo(httpPost, ActionListener.noop());
        var apiKeyHeader = httpPost.getFirstHeader(API_KEY_HEADER);

        assertThat(apiKeyHeader.getValue(), is(apiKey.toString()));
    }

    public void testCreateSecretsApplier_EntraId() {
        var entraId = randomSecureStringOfLength(10);
        var httpPost = new HttpPost();
        var secretSettings = new AzureOpenAiEntraIdApiKeySecrets(null, entraId);
        var secretsApplier = AzureOpenAiSecretsFactory.createSecretsApplier(
            TEST_INFERENCE_ID,
            threadPool,
            secretSettings,
            AzureOpenAiCompletionServiceSettingsTests.createRandomWithoutOAuth2()
        );

        assertNotNull(secretsApplier);

        secretsApplier.applyTo(httpPost, ActionListener.noop());
        var authHeader = httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION);

        assertThat(authHeader.getValue(), is("Bearer " + entraId));
    }

    public void testCreateSecretsApplier_OAuth2ClientSecret_DoesNotSetAuthHeaders_WhenOAuth2SettingsAreAbsent() {
        var secretSettings = new AzureOpenAiOAuth2Secrets(randomSecureStringOfLength(10));
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretsFactory.createSecretsApplier(
                TEST_INFERENCE_ID,
                threadPool,
                secretSettings,
                AzureOpenAiCompletionServiceSettingsTests.createRandomWithoutOAuth2()
            )
        );

        assertThat(thrownException.getMessage(), containsString("OAuth2 requires the fields [client_id, scopes, tenant_id], to be set."));
    }

    public void testCreateSecretsApplier_ApiKey_ThrowsException_WhenOAuth2SettingsArePresent() {
        var secretSettings = new AzureOpenAiEntraIdApiKeySecrets(randomSecureStringOfLength(10), null);
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretsFactory.createSecretsApplier(
                TEST_INFERENCE_ID,
                threadPool,
                secretSettings,
                AzureOpenAiCompletionServiceSettingsTests.createRandom(AzureOpenAiOAuth2SettingsTests.createRandom())
            )
        );

        assertThat(thrownException.getMessage(), containsString(USE_CLIENT_SECRET_ERROR));
    }

    public void testCreateSecretsApplier_EntraId_ThrowsException_WhenOAuth2SettingsArePresent() {
        var secretSettings = new AzureOpenAiEntraIdApiKeySecrets(null, randomSecureStringOfLength(10));
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiSecretsFactory.createSecretsApplier(
                TEST_INFERENCE_ID,
                threadPool,
                secretSettings,
                AzureOpenAiCompletionServiceSettingsTests.createRandom(AzureOpenAiOAuth2SettingsTests.createRandom())
            )
        );

        assertThat(thrownException.getMessage(), containsString(USE_CLIENT_SECRET_ERROR));
    }
}
