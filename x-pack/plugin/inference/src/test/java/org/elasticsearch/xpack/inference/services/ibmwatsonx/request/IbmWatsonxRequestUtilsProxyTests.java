/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpSettings;
import org.elasticsearch.xpack.inference.external.http.MockHttpProxyServer;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class IbmWatsonxRequestUtilsProxyTests extends ESTestCase {
    private static final String INFERENCE_ID = "test-inference-id";

    private MockHttpProxyServer proxyServer;

    @Before
    public void init() {
        proxyServer = new MockHttpProxyServer();
    }

    @After
    public void shutdown() throws IOException {
        proxyServer.close();
    }

    public void testDecorateWithBearerToken_EstablishesTunnelThroughProxy_WhenProxyIsConfigured() {
        var settings = Settings.builder()
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_HOST_KEY, proxyServer.getHost())
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_PORT_KEY, proxyServer.getPort())
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_SCHEME_KEY, "http")
            .build();
        var proxySettings = HttpSettings.ProxySettings.fromSettings(settings);

        var httpPost = new HttpPost("https://localhost/ml/v1/text/embeddings");
        var secretSettings = new DefaultSecretSettings(new SecureString("api-key".toCharArray()));

        // the proxy rejects the tunnel handshake with a 502, which the bearer token client surfaces as the response, so the request
        // must fail instead of reaching IBM's IAM endpoint directly
        var exception = expectThrows(
            RetryException.class,
            () -> IbmWatsonxRequestUtils.decorateWithBearerToken(httpPost, secretSettings, INFERENCE_ID, proxySettings)
        );
        assertThat(exception.getCause().getMessage(), containsString("status [502]"));

        // Assert that the bearer token client asked the proxy to open a tunnel to IBM's IAM endpoint
        assertThat(proxyServer.proxiedRequestLines(), not(empty()));
        for (var requestLine : proxyServer.proxiedRequestLines()) {
            assertThat(requestLine, containsString("CONNECT"));
            assertThat(requestLine, containsString(URI.create(IbmWatsonxRequestUtils.BEARER_TOKEN_GEN_URL).getHost()));
        }
    }
}
