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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.secrets.NoopSecretsApplier;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OpenAiSecretsFactoryTests extends ESTestCase {

    public void testCreateSecretsApplier_NullSecrets_ReturnsNoopSecretsApplier() {
        var secretsApplier = OpenAiSecretsFactory.createSecretsApplier(null);

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

        var secretsApplier = OpenAiSecretsFactory.createSecretsApplier(secretSettings);

        assertNotNull(secretsApplier);

        secretsApplier.applyTo(httpPost, ActionListener.noop());
        var authHeader = httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION);

        assertThat(authHeader.getValue(), is("Bearer " + apiKey));
    }
}
