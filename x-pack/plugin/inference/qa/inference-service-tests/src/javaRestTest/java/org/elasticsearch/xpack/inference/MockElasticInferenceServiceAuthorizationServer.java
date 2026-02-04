/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.EIS_EMPTY_RESPONSE;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.getEisAuthorizationResponseWithMultipleEndpoints;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.getEisElserAuthorizationResponse;

public class MockElasticInferenceServiceAuthorizationServer implements TestRule {

    private static final Logger logger = LogManager.getLogger(MockElasticInferenceServiceAuthorizationServer.class);
    private final MockWebServer webServer = new MockWebServer();

    /**
     * Ensure that the mock EIS server the initial number of authorized responses queued up. This is particularly useful when
     * authorization requests are made during a node bootup.
     * @param numInitialResponses the number of authorized responses to enqueue upon construction
     */
    public void init(int numInitialResponses) {
        for (int i = 0; i < numInitialResponses; i++) {
            // This call needs to happen outside the constructor to avoid an error for a this-escape
            enqueueAuthorizeAllModelsResponse();
        }
    }

    /**
     * Enqueue empty responses. Use this when the mock should return no preconfigured endpoints (e.g. until mixed cluster state).
     * @param count the number of empty responses to enqueue
     */
    public void enqueueEmptyResponses(int count) {
        for (int i = 0; i < count; i++) {
            enqueueEmptyResponse();
        }
    }

    /**
     * Enqueue a single empty response (inference_endpoints: []).
     */
    public void enqueueEmptyResponse() {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EIS_EMPTY_RESPONSE));
    }

    /**
     * Enqueue a preconfigured endpoint response that includes EndpointMetadata (e.g. elser-2).
     * When stored in a mixed cluster this will cause a mapping exception because old nodes lack the metadata field.
     */
    public void enqueuePreconfiguredEndpointResponse() {
        var authResponseBody = getEisElserAuthorizationResponse(getUrl()).responseJson();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(authResponseBody));
    }

    /**
     * Enqueue a preconfigured endpoint response with a callback that is invoked when the webserver receives the request.
     * This allows tests to know when the auth request was made so they can assertBusy for endpoint persistence.
     * @param requestReceived set to true when the mock server receives the authorization request
     */
    public void enqueuePreconfiguredEndpointResponseWithRequestReceivedCallback(AtomicBoolean requestReceived) {
        var authResponseBody = getEisElserAuthorizationResponse(getUrl()).responseJson();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody((MockRequest request) -> {
            requestReceived.set(true);
            return authResponseBody;
        }));
    }

    public void enqueueAuthorizeAllModelsResponse() {
        var authResponseBody = getEisAuthorizationResponseWithMultipleEndpoints("ignored").responseJson();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(authResponseBody));
    }

    public MockWebServer getWebServer() {
        return webServer;
    }

    public String getUrl() {
        return format("http://%s:%s", webServer.getHostName(), webServer.getPort());
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    logger.info("Starting mock EIS gateway");
                    webServer.start();
                    logger.info(Strings.format("Started mock EIS gateway with address: %s", getUrl()));
                } catch (Exception e) {
                    logger.warn("Failed to start mock EIS gateway", e);
                }

                try {
                    statement.evaluate();
                } finally {
                    logger.info(Strings.format("Stopping mock EIS gateway address: %s", getUrl()));
                    webServer.close();
                }
            }
        };
    }
}
