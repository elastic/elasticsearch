/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;

import java.io.IOException;
import java.security.KeyPairGenerator;
import java.util.Base64;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class GoogleVertexAiRequestUtilsTests extends ESTestCase {

    // The Application Default Credentials failure path always includes this phrase; see
    // DefaultCredentialsProvider#getDefaultCredentials in google-auth-library-oauth2-http.
    private static final String ADC_UNAVAILABLE_MARKER = "Application Default Credentials";

    public void testDecorateWithBearerToken_WithSecretSettings_UsesServiceAccountCredentials() throws Exception {
        var secretSettings = new GoogleVertexAiSecretSettings(new SecureString(randomServiceAccountJson().toCharArray()));
        var httpPost = new HttpPost("https://example.com");

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> GoogleVertexAiRequestUtils.decorateWithBearerToken(httpPost, secretSettings)
        );

        assertThat(exception.status(), is(RestStatus.FORBIDDEN));
        // The fake service account key points at an unroutable token endpoint, so the failure
        // must come from a refused token exchange, not from Application Default Credentials.
        assertThat(exception.getCause(), instanceOf(IOException.class));
        assertThat(exception.getCause().getMessage(), not(containsString(ADC_UNAVAILABLE_MARKER)));
    }

    public void testDecorateWithBearerToken_WithoutSecretSettings_FallsBackToApplicationDefaultCredentials() {
        var httpPost = new HttpPost("https://example.com");

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> GoogleVertexAiRequestUtils.decorateWithBearerToken(httpPost, null)
        );

        assertThat(exception.status(), is(RestStatus.FORBIDDEN));
        // A null secretSettings must route to ApplicationDefaultCredentials rather than throwing
        // a NullPointerException from a direct dereference.
        assertThat(exception.getCause(), not(instanceOf(NullPointerException.class)));
        assertThat(exception.getCause(), instanceOf(IOException.class));
    }

    /**
     * Builds a syntactically valid Google service account JSON key with a random RSA key pair and
     * a deliberately unroutable {@code token_uri}, so that any attempt to actually exchange it for
     * an access token fails fast and locally rather than requiring real network access or real
     * Google credentials.
     */
    private static String randomServiceAccountJson() throws Exception {
        var keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        var encodedPrivateKey = Base64.getEncoder().encodeToString(keyPair.getPrivate().getEncoded());
        return Strings.format("""
            {
              "type": "service_account",
              "project_id": "test-project",
              "private_key_id": "test-key-id",
              "private_key": "-----BEGIN PRIVATE KEY-----\\n%s\\n-----END PRIVATE KEY-----\\n",
              "client_email": "test@test-project.iam.gserviceaccount.com",
              "client_id": "test-client-id",
              "token_uri": "http://127.0.0.1:1/oauth2/token"
            }""", encodedPrivateKey);
    }
}
