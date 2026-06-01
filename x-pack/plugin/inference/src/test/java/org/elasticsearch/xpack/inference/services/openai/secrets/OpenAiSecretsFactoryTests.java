/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.secrets;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.oauth2.NoopTokenCache;
import org.elasticsearch.xpack.inference.common.oauth2.TokenCache;
import org.elasticsearch.xpack.inference.common.secrets.NoopSecretsApplier;
import org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.junit.After;
import org.junit.Before;

import java.net.URI;
import java.util.List;

import static org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings.REQUIRED_FIELDS_DESCRIPTION;
import static org.elasticsearch.xpack.inference.services.openai.secrets.OpenAiSecretsFactory.USE_CLIENT_SECRET_ERROR;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OpenAiSecretsFactoryTests extends ESTestCase {

    private static final String INFERENCE_ID = "inference-id";
    private static final URI AUTH_URL = URI.create("https://idp.example.com/token");
    private static final String CLIENT_ID = "client-id";
    private static final List<String> SCOPES = List.of("api");

    private ThreadPool threadPool;
    private TokenCache tokenCache;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        tokenCache = new NoopTokenCache();
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testCreateSecretsApplier_NullSecrets_ReturnsNoopSecretsApplier() {
        var serviceSettings = serviceSettingsWith(null);
        var secretsApplier = OpenAiSecretsFactory.createSecretsApplier(INFERENCE_ID, threadPool, tokenCache, null, serviceSettings);

        assertThat(secretsApplier, sameInstance(NoopSecretsApplier.INSTANCE));

        var listener = new TestPlainActionFuture<HttpRequestBase>();
        var httpPost = new HttpPost();
        secretsApplier.applyTo(httpPost, listener);

        var resultRequest = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(resultRequest, sameInstance(httpPost));
        assertThat(resultRequest.getAllHeaders(), emptyArray());
    }

    public void testCreateSecretsApplier_ApiKey_SetsBearerAuthorizationHeader() {
        var apiKey = randomSecureStringOfLength(10);
        var httpPost = new HttpPost();
        var secretSettings = new DefaultSecretSettings(apiKey);
        var serviceSettings = serviceSettingsWith(null);

        var secretsApplier = OpenAiSecretsFactory.createSecretsApplier(
            INFERENCE_ID,
            threadPool,
            tokenCache,
            secretSettings,
            serviceSettings
        );

        assertNotNull(secretsApplier);

        secretsApplier.applyTo(httpPost, ActionListener.noop());
        var authHeader = httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION);

        assertThat(authHeader.getValue(), is("Bearer " + apiKey));
    }

    public void testCreateSecretsApplier_OAuth2Secrets_ReturnsOAuth2Applier() {
        var serviceSettings = serviceSettingsWith(new OpenAiOAuth2Settings(CLIENT_ID, SCOPES, AUTH_URL));
        var secureString = randomSecureStringOfLength(10);
        var secrets = new OpenAiOAuth2SecretsSettings(secureString);

        var applier = OpenAiSecretsFactory.createSecretsApplier(INFERENCE_ID, threadPool, tokenCache, secrets, serviceSettings);

        assertThat(applier, instanceOf(OpenAiOAuth2Applier.class));
    }

    public void testCreateSecretsApplier_OAuth2SecretWithoutSettings_ThrowsValidation() {
        var serviceSettings = serviceSettingsWith(null);
        var secureString = randomSecureStringOfLength(10);
        var secrets = new OpenAiOAuth2SecretsSettings(secureString);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiSecretsFactory.createSecretsApplier(INFERENCE_ID, threadPool, tokenCache, secrets, serviceSettings)
        );
        assertThat(thrownException.getMessage(), containsString(REQUIRED_FIELDS_DESCRIPTION));
    }

    public void testCreateSecretsApplier_ApiKeyWithOAuth2Settings_ThrowsValidation() {
        var serviceSettings = serviceSettingsWith(new OpenAiOAuth2Settings(CLIENT_ID, SCOPES, AUTH_URL));
        var apiKey = new DefaultSecretSettings(randomSecureStringOfLength(10));

        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiSecretsFactory.createSecretsApplier(INFERENCE_ID, threadPool, tokenCache, apiKey, serviceSettings)
        );
        assertThat(thrownException.getMessage(), containsString(USE_CLIENT_SECRET_ERROR));
    }

    private static OpenAiServiceSettings serviceSettingsWith(OpenAiOAuth2Settings oAuth2Settings) {
        return new OpenAiChatCompletionServiceSettings(randomAlphaOfLength(5), null, null, null, null, oAuth2Settings);
    }
}
