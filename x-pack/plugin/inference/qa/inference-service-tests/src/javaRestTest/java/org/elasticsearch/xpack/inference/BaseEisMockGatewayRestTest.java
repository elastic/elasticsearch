/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getModel;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings.CCM_SUPPORTED_ENVIRONMENT;

/**
 * Base for integration tests that point a cluster at a {@link MockElasticInferenceServiceAuthorizationServer} standing in for the
 * EIS gateway, so the requests the node sends to EIS can be captured and inspected.
 * <p>
 * JUnit requires class rules to be static and each suite needs its own cluster settings, so subclasses declare the mock server, the
 * cluster and the {@link org.junit.rules.RuleChain} that starts the server before the cluster. {@link #eisClusterBuilder} supplies
 * the settings every suite shares; a subclass adds security, authorization and plugin settings on top and returns the result from
 * {@link #cluster()}.
 */
public abstract class BaseEisMockGatewayRestTest extends ESRestTestCase {

    /**
     * A cluster that talks to the mock gateway. Periodic authorization is off so the mock only receives the requests a test
     * triggers, and CCM is off so authorization does not try to determine whether CCM is enabled.
     */
    protected static LocalClusterSpecBuilder<ElasticsearchCluster> eisClusterBuilder(
        MockElasticInferenceServiceAuthorizationServer mockEISServer
    ) {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.license.self_generated.type", "trial")
            .setting(CCM_SUPPORTED_ENVIRONMENT.getKey(), "false")
            .setting(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), mockEISServer::getUrl)
            .setting(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), "false");
    }

    protected abstract ElasticsearchCluster cluster();

    @Override
    protected String getTestRestCluster() {
        return cluster().getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Keep the preconfigured EIS endpoints between tests; wiping the cluster would delete the inference indices.
        return true;
    }

    /**
     * A minimal single-chunk SSE stream for the mock gateway to answer a chat completion request with. The content is irrelevant;
     * it only needs to be a valid stream so the request completes and the outgoing EIS request is captured by the mock.
     */
    protected static MockResponse streamResponse(String model) {
        var body = Strings.format("""
            data: {"id":"1","object":"completion","created":1,"model":"%s",\
            "choices":[{"finish_reason":"stop","index":0,"delta":{"role":"assistant","content":"ok"}}]}

            data: [DONE]

            """, model);
        return new MockResponse().setResponseCode(200).setBody(body);
    }

    /**
     * The node stores the preconfigured endpoints a few seconds after startup. {@code getModel} fails with a
     * {@link ResponseException} until then, which {@code assertBusy} would not retry on its own.
     */
    protected static void awaitPreconfiguredEndpoint(String inferenceId) throws Exception {
        assertBusy(() -> {
            try {
                getModel(inferenceId);
            } catch (ResponseException e) {
                fail("endpoint [" + inferenceId + "] not available yet: " + e.getMessage());
            }
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Fires a {@code POST _inference/chat_completion/{inferenceId}/_stream} request and blocks until the response is fully consumed.
     * The purpose is to make the node forward the request to the mock gateway; the SSE events are not asserted.
     */
    protected void executeStreamRequest(String inferenceId, String body) throws Exception {
        var request = new Request("POST", "_inference/chat_completion/" + inferenceId + "/_stream");
        request.setJsonEntity(body);

        var responseConsumer = new AsyncInferenceResponseConsumer();
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setHttpAsyncResponseConsumerFactory(() -> responseConsumer).build());
        var latch = new CountDownLatch(1);
        var failure = new AtomicReference<Exception>();
        client().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                failure.set(exception);
                latch.countDown();
            }
        });
        assertTrue(latch.await(TEST_REQUEST_TIMEOUT.getSeconds(), TimeUnit.SECONDS));
        assertNull(failure.get());
    }

    /**
     * The JSON body of the most recent request the mock gateway received.
     */
    protected static Map<String, Object> lastRequestBody(MockElasticInferenceServiceAuthorizationServer mockEISServer) throws IOException {
        var body = mockEISServer.getWebServer().requests().getLast().getBody();
        return entityAsMap(new StringEntity(body, ContentType.APPLICATION_JSON));
    }
}
