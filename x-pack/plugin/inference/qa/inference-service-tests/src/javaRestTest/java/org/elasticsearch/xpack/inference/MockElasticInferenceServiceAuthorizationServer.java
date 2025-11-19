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

    public void enqueueAuthorizeAllModelsResponse() {
        String responseJson = """
            {
              "inference_endpoints": [
                {
                  "id": ".rainbow-sprinkles-elastic",
                  "model_name": "rainbow-sprinkles",
                  "task_type": "chat_completion",
                  "status": "ga",
                  "properties": [
                    "multilingual"
                  ],
                  "release_date": "2024-05-01",
                  "end_of_life_date": "2025-12-31"
                },
                {
                  "id": ".gp-llm-v2-chat_completion",
                  "model_name": "gp-llm-v2",
                  "task_type": "chat_completion",
                  "status": "ga",
                  "properties": [
                    "multilingual"
                  ],
                  "release_date": "2024-05-01",
                  "end_of_life_date": "2025-12-31"
                },
                {
                  "id": ".elser-2-elastic",
                  "model_name": "elser_model_2",
                  "task_type": "sparse_embedding",
                  "status": "preview",
                  "properties": [
                    "english"
                  ],
                  "release_date": "2024-05-01",
                  "configuration": {
                    "chunking_settings": {
                      "strategy": "sentence",
                      "max_chunk_size": 250,
                      "sentence_overlap": 1
                    }
                  }
                },
                {
                  "id": ".jina-embeddings-v3",
                  "model_name": "jina-embeddings-v3",
                  "task_type": "text_embedding",
                  "status": "beta",
                  "properties": [
                    "multilingual",
                    "open-weights"
                  ],
                  "release_date": "2024-05-01",
                  "configuration": {
                    "similarity": "cosine",
                    "dimensions": 1024,
                    "element_type": "float",
                    "chunking_settings": {
                      "strategy": "word",
                      "max_chunk_size": 500,
                      "overlap": 2
                    }
                  }
                },
                {
                  "id": ".elastic-rerank-v1",
                  "model_name": "elastic-rerank-v1",
                  "task_type": "rerank",
                  "status": "preview",
                  "properties": [],
                  "release_date": "2024-05-01"
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
