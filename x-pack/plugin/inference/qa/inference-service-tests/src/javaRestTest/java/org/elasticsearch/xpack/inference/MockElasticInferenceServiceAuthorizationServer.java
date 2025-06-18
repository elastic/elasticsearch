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
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static org.elasticsearch.core.Strings.format;

public class MockElasticInferenceServiceAuthorizationServer implements TestRule {

    private static final Logger logger = LogManager.getLogger(MockElasticInferenceServiceAuthorizationServer.class);
    private final MockWebServer webServer = new MockWebServer();

    public static MockElasticInferenceServiceAuthorizationServer enabledWithRainbowSprinklesAndElser() {
        var server = new MockElasticInferenceServiceAuthorizationServer();

        server.enqueueAuthorizeAllModelsResponse();
        return server;
    }

    public void enqueueAuthorizeAllModelsResponse() {
        String responseJson = """
            {
                "models": [
                    {
                      "model_name": "rainbow-sprinkles",
                      "task_types": ["chat"]
                    },
                    {
                      "model_name": "elser-v2",
                      "task_types": ["embed/text/sparse"]
                    }
                ]
            }
            """;

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
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
