/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpSettings;
import org.elasticsearch.xpack.inference.external.http.MockHttpProxyServer;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class AmazonBedrockInferenceClientProxyTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String REGION = "us-east-1";

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

    public void testConverse_EstablishesTunnelThroughProxy_WhenProxyIsConfigured() {
        var settings = Settings.builder()
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_HOST_KEY, proxyServer.getHost())
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_PORT_KEY, proxyServer.getPort())
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_SCHEME_KEY, "http")
            .build();
        var proxySettings = HttpSettings.ProxySettings.fromSettings(settings);

        var client = AmazonBedrockInferenceClient.create(createModel(), TIMEOUT, threadPool, proxySettings);
        try {
            PlainActionFuture<ConverseResponse> listener = new PlainActionFuture<>();
            client.converse(ConverseRequest.builder().modelId("amazon.titan-text-express-v1").build(), listener);

            expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

            // Assert that client asked proxy to open a tunnel
            // SDK might retry, therefore we check for multiple CONNECT requests
            assertThat(proxyServer.proxiedRequestLines(), not(empty()));
            for (var requestLine : proxyServer.proxiedRequestLines()) {
                assertThat(requestLine, containsString("CONNECT"));
                assertThat(requestLine, containsString(REGION));
            }
        } finally {
            client.close();
        }
    }

    public void testClientCreation_DoesNotUseProxy_WhenNoProxyIsConfigured() {
        var client = AmazonBedrockInferenceClient.create(createModel(), TIMEOUT, threadPool, null);
        client.close();

        // Proxy didn't receive any requests on client creation/closing
        assertThat(proxyServer.proxiedRequestLines(), empty());
    }

    private static AmazonBedrockModel createModel() {
        return AmazonBedrockChatCompletionModelTests.createCompletionModel(
            "inference-id",
            REGION,
            "amazon.titan-text-express-v1",
            AmazonBedrockProvider.AMAZONTITAN,
            "access-key",
            "secret-key"
        );
    }
}
