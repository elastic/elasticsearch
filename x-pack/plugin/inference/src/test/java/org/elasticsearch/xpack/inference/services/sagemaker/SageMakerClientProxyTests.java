/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker;

import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointRequest;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.external.http.HttpSettings;
import org.elasticsearch.xpack.inference.external.http.MockHttpProxyServer;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterService;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class SageMakerClientProxyTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String REGION = "us-east-1";
    private static final String INFERENCE_ID = "test-inference-id";

    private MockHttpProxyServer proxyServer;
    private ThreadPool threadPool;

    @Before
    public void init() {
        proxyServer = new MockHttpProxyServer();
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
        proxyServer.close();
    }

    public void testInvoke_EstablishesTunnelThroughProxy_WhenProxyIsConfigured() throws Exception {
        var settings = Settings.builder()
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_HOST_KEY, proxyServer.getHost())
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_PORT_KEY, proxyServer.getPort())
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_SCHEME_KEY, "http")
            .build();

        try (var client = new SageMakerClient(createFactory(settings), threadPool)) {
            PlainActionFuture<InvokeEndpointResponse> listener = new PlainActionFuture<>();
            var request = InvokeEndpointRequest.builder().endpointName("test-endpoint").build();
            client.invoke(regionAndSecrets(), request, TIMEOUT, INFERENCE_ID, listener);

            expectThrows(Exception.class, () -> listener.actionGet(TIMEOUT));

            // Assert that client asked proxy to open a tunnel
            // SDK might retry, therefore we check for multiple CONNECT requests
            assertThat(proxyServer.proxiedRequestLines(), not(empty()));
            for (var requestLine : proxyServer.proxiedRequestLines()) {
                assertThat(requestLine, containsString("CONNECT"));
                assertThat(requestLine, containsString(REGION));
            }
        }
    }

    public void testClientCreation_DoesNotUseProxy_WhenNoProxyIsConfigured() throws Exception {
        createFactory(Settings.EMPTY).load(regionAndSecrets()).close();

        // Proxy didn't receive any requests on client creation/closing
        assertThat(proxyServer.proxiedRequestLines(), empty());
    }

    private static SageMakerClient.Factory createFactory(Settings settings) {
        return new SageMakerClient.Factory(new HttpSettings(settings, mockClusterService(settings)));
    }

    private static SageMakerClient.RegionAndSecrets regionAndSecrets() {
        return new SageMakerClient.RegionAndSecrets(
            REGION,
            new AwsSecretSettings(new SecureString("access-key"), new SecureString("secret-key"))
        );
    }
}
